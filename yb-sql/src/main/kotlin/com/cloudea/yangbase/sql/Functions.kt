package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.ZoneOffset

/**
 * **内置标量函数库**（MySQL 兼容面）。
 *
 * ## 为什么要有一张注册表
 *
 * 在此之前，函数分派散落在四处（`Eval.evalFunction` 的 `when`、`JsonFunctions.NAMES`、
 * Parser 的聚合名单、Window 的窗口名单）。每加一个函数都要在"是否要改解析器"上重新判断一次，
 * 而**最容易漏的是参数个数校验**——`UPPER()` 会抛 `IndexOutOfBoundsException`，
 * 被协议层兜成 1105 并把栈打到 stderr：用户看到的是"数据库自己有缺陷"，
 * 而不是"我的 SQL 写错了"。
 *
 * 所以这里的约定是：**每个函数都要声明参数个数与 NULL 语义**，由框架统一执行：
 *
 * - [Fn.strict] = 任一参数为 NULL 就返回 NULL（MySQL 里绝大多数函数如此：
 *   `UPPER(NULL)`、`CONCAT('a', NULL)`、`ROUND(NULL)` 都是 NULL）。
 *   不标 strict 的是控制流与少数特例（`IF` / `IFNULL` / `COALESCE` / `CONCAT_WS` / `ISNULL`），
 *   它们**必须**在 NULL 上做点别的。
 * - 参数个数不符 → `1210 ER_WRONG_ARGUMENTS`，文案带函数名与期望个数。
 *
 * ## 与 MySQL 的差异（都在这里记着，别处不再重复）
 *
 * - **没有 warning 通道**：MySQL 里"参数越界/转换失败"多数是 warning + 尽力而为的值
 *   （`CAST('abc' AS SIGNED)` = 0），本系统一律**报错**（1292 / 1690）。
 *   理由与 §11.3 的立场一致：静默给一个"看起来正常"的值，比报错危险得多。
 * - **时间一律按 UTC 解释与渲染**（`Datum.parseDateTime` / `formatDateTime`），
 *   与 MySQL 的 session `time_zone` 无关（`SET time_zone` 目前是空实现）。
 * - `DATE_FORMAT` / `STR_TO_DATE` 支持 MySQL 的格式符集合，但 `%f`（微秒）在本系统里是毫秒。
 */
internal object Builtins {

    /** 一个内置函数：参数个数区间 + NULL 语义 + 实现。 */
    class Fn(
        val name: String,
        val minArgs: Int,
        val maxArgs: Int,
        /** 任一参数为 NULL 时直接返回 NULL（MySQL 的默认行为）。 */
        val strict: Boolean,
        val impl: (List<Datum>, EvalContext) -> Datum,
    ) {
        val variadic: Boolean get() = maxArgs == Int.MAX_VALUE
    }

    /**
     * 名字 → **按参数个数区分的多个实现**。
     *
     * 一个名字对应多个签名是 MySQL 的常态（`ROUND(x)` / `ROUND(x,d)`、
     * `DATE_ADD(d,n)` / `DATE_ADD(d,n,UNIT)`）。若用一个 `Fn` 覆盖注册，
     * 后注册的那个会把先注册的**悄悄顶掉**——症状是某个写法突然报"参数个数不对"。
     */
    private val registry = HashMap<String, MutableList<Fn>>()

    fun names(): Set<String> = registry.keys

    /** 这个名字认识吗（不管参数个数）。 */
    fun lookup(name: String): List<Fn>? = registry[name]

    /**
     * 调用。名字不认识返回 null，由调用方决定怎么报错
     * （`Eval` 要区分"未知函数"与"JSON 函数"两种文案）。
     */
    fun call(name: String, args: List<Datum>, ctx: EvalContext): Datum? {
        val overloads = registry[name] ?: return null
        // 先按参数个数挑签名；挑不到再报"个数不对"，用最接近的那个的区间做提示
        val fn = overloads.firstOrNull { args.size >= it.minArgs && args.size <= it.maxArgs }
            ?: overloads.minByOrNull { kotlin.math.abs(it.minArgs - args.size) }!!
        if (args.size < fn.minArgs || args.size > fn.maxArgs) {
            val expected = when {
                fn.minArgs == fn.maxArgs -> "${fn.minArgs} 个"
                fn.variadic -> "至少 ${fn.minArgs} 个"
                else -> "${fn.minArgs}~${fn.maxArgs} 个"
            }
            throw SqlException(
                MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
                "FUNCTION $name 需要 $expected 参数，实际 ${args.size} 个",
            )
        }
        if (fn.strict && args.any { it is Datum.Null }) return Datum.Null
        return fn.impl(args, ctx)
    }

    // ── 注册辅助 ──────────────────────────────────────────────────

    private fun reg(name: String, min: Int, max: Int, strict: Boolean = true, impl: (List<Datum>, EvalContext) -> Datum) {
        registry.getOrPut(name) { ArrayList() }.add(Fn(name, min, max, strict, impl))
    }

    /** 定长参数、严格 NULL：`impl` 拿到的一定是非 NULL 的值。 */
    private fun regN(vararg names: String, arity: Int, impl: (List<Datum>) -> Datum) {
        for (n in names) reg(n, arity, arity, strict = true) { a, _ -> impl(a) }
    }

    private fun reg1(vararg names: String, impl: (Datum) -> Datum) =
        regN(*names, arity = 1) { impl(it[0]) }

    private fun reg2(vararg names: String, impl: (Datum, Datum) -> Datum) =
        regN(*names, arity = 2) { impl(it[0], it[1]) }

    private fun reg3(vararg names: String, impl: (Datum, Datum, Datum) -> Datum) =
        regN(*names, arity = 3) { impl(it[0], it[1], it[2]) }

    // ── 类型转换辅助 ──────────────────────────────────────────────

    /**
     * 取值当字符串用。
     *
     * ⚠️ `Datum.Bin` 要按**字节**当字符串（latin1 语义），而不是用
     * `Datum.Bin.toString()` 的 `0x…` 十六进制表示——后者会让
     * `LENGTH(UNHEX('0102'))` 返回 6（"0x0102" 的长度）而不是 2。
     */
    private fun str(d: Datum): String =
        if (d is Datum.Bin) String(d.v, Charsets.ISO_8859_1) else d.asString()

    /** 整数语义（MySQL 里 `LENGTH` 之外的数值函数多半按浮点算，这里区分开）。 */
    private fun dbl(d: Datum): Double = when (d) {
        is Datum.I64 -> d.v.toDouble()
        is Datum.F64 -> d.v
        is Datum.Dec -> d.unscaled.toDouble() / Datum.pow10(d.scale)
        is Datum.Bool -> if (d.v) 1.0 else 0.0
        is Datum.Str -> d.v.trim().toDoubleOrNull() ?: throw badNumber(d.v)
        is Datum.Ts -> d.millis.toDouble()
        is Datum.Bin -> throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "二进制值不能当作数字使用",
        )
        Datum.Null -> throw IllegalStateException("strict 函数不该拿到 NULL")
    }

    private fun lng(d: Datum): Long = when (d) {
        is Datum.I64 -> d.v
        is Datum.Bool -> if (d.v) 1L else 0L
        is Datum.Dec -> if (d.scale == 0) d.unscaled else d.unscaled / Datum.pow10Long(d.scale)
        is Datum.F64 -> d.v.toLong()
        is Datum.Ts -> d.millis
        is Datum.Str -> d.v.trim().toLongOrNull() ?: throw badNumber(d.v)
        is Datum.Bin -> throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "二进制值不能当作整数使用",
        )
        Datum.Null -> throw IllegalStateException("strict 函数不该拿到 NULL")
    }

    private fun badNumber(text: String) = SqlException(
        MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "无法把 '$text' 当数字使用",
    )

    /** 数值结果：整数输入保持整数（MySQL 的 `ABS(-1)` 是整数），其余走浮点。 */
    private fun numResult(originals: List<Datum>, v: Double): Datum = when {
        originals.all { it is Datum.I64 || it is Datum.Bool || it is Datum.Dec } ->
            Datum.I64(v.toLong())
        else -> Datum.F64(v)
    }

    private fun isExact(d: Datum) = d is Datum.I64 || d is Datum.Bool || d is Datum.Dec

    // ── 注册表 ────────────────────────────────────────────────────

    init {
        registerControl()
        registerString()
        registerNumeric()
        registerDateTime()
        registerCasts()
    }

    // ── 控制流与 NULL 处理（都不 strict：它们在 NULL 上做别的事）────

    private fun registerControl() {
        // IF(cond, a, b)：只关心条件是否为真；NULL 条件按"假"处理（MySQL 同）
        reg("IF", 3, 3, strict = false) { a, _ ->
            if (a[0] is Datum.Null) a[2] else if (Eval.truthy(a[0])) a[1] else a[2]
        }
        reg("IFNULL", 2, 2, strict = false) { a, _ -> if (a[0] is Datum.Null) a[1] else a[0] }
        reg("NVL", 2, 2, strict = false) { a, _ -> if (a[0] is Datum.Null) a[1] else a[0] }
        // NULLIF(a,b)：相等则 NULL。比较用 SQL 的比较语义（含类型转换）
        reg("NULLIF", 2, 2, strict = false) { a, _ ->
            if (a[0] is Datum.Null) Datum.Null
            else if (Eval.eqPublic(a[0], a[1])) Datum.Null
            else a[0]
        }
        reg("COALESCE", 1, Int.MAX_VALUE, strict = false) { a, _ ->
            a.firstOrNull { it !is Datum.Null } ?: Datum.Null
        }
        // GREATEST / LEAST：MySQL 里任一参数为 NULL 则结果为 NULL（严格）
        reg("GREATEST", 2, Int.MAX_VALUE) { a, _ -> a.reduce { x, y -> if (x.compareTo(y) >= 0) x else y } }
        reg("LEAST", 2, Int.MAX_VALUE) { a, _ -> a.reduce { x, y -> if (x.compareTo(y) <= 0) x else y } }
        reg("ISNULL", 1, 1, strict = false) { a, _ -> Datum.Bool(a[0] is Datum.Null) }
    }

    // ── 字符串 ────────────────────────────────────────────────────

    private fun registerString() {
        reg1("UPPER", "UCASE") { Datum.Str(str(it).uppercase()) }
        reg1("LOWER", "LCASE") { Datum.Str(str(it).lowercase()) }

        // ⚠️ LENGTH 是**字节数**（utf8mb4 下一个汉字 3 字节），CHAR_LENGTH 是字符数。
        // 曾经两者都按 Kotlin 的 `String.length`（UTF-16 码元）算，于是多字节内容
        // 与 MySQL 的结果不同——而 `LENGTH()` 常被用来判断"是否超长"，差 3 倍是致命的
        reg1("LENGTH") { Datum.I64(str(it).toByteArray(Charsets.UTF_8).size.toLong()) }
        reg1("CHAR_LENGTH", "CHARACTER_LENGTH") { Datum.I64(str(it).length.toLong()) }
        reg1("OCTET_LENGTH") { Datum.I64(str(it).toByteArray(Charsets.UTF_8).size.toLong()) }
        reg1("BIT_LENGTH") { Datum.I64(str(it).toByteArray(Charsets.UTF_8).size.toLong() * 8) }

        // CONCAT 在任一参数为 NULL 时整体为 NULL（MySQL 语义）。CONCAT_WS 跳过 NULL，
        // 但分隔符为 NULL 时整体为 NULL
        reg("CONCAT", 1, Int.MAX_VALUE) { a, _ -> Datum.Str(a.joinToString("") { str(it) }) }
        reg("CONCAT_WS", 2, Int.MAX_VALUE, strict = false) { a, _ ->
            if (a[0] is Datum.Null) Datum.Null
            else Datum.Str(a.drop(1).filter { it !is Datum.Null }.joinToString(str(a[0])) { str(it) })
        }

        reg("SUBSTRING", 2, 3) { a, _ -> substring(str(a[0]), lng(a[1]), a.getOrNull(2)) }
        reg("SUBSTR", 2, 3) { a, _ -> substring(str(a[0]), lng(a[1]), a.getOrNull(2)) }
        reg("MID", 2, 3) { a, _ -> substring(str(a[0]), lng(a[1]), a.getOrNull(2)) }
        reg2("LEFT") { s, n -> Datum.Str(str(s).take(lng(n).coerceAtLeast(0).coerceAtMost(str(s).length.toLong()).toInt())) }
        reg2("RIGHT") { s, n ->
            val t = str(s); val k = lng(n).coerceAtLeast(0).coerceAtMost(t.length.toLong()).toInt()
            Datum.Str(t.substring(t.length - k))
        }
        reg1("REVERSE") { Datum.Str(str(it).reversed()) }
        reg2("REPEAT") { s, n ->
            val k = lng(n)
            if (k <= 0) Datum.Str("") else Datum.Str(str(s).repeat(k.coerceAtMost(MAX_REPEAT).toInt()))
        }
        reg1("SPACE") { Datum.Str(" ".repeat(lng(it).coerceIn(0, MAX_REPEAT).toInt())) }

        // TRIM 系列：单参数形式去空格；TRIM(BOTH 'x' FROM s) 那种语法形式由解析器
        // 折成 TRIM(s, 'x') 之类的调用（见 Parser 里的 SPECIAL_TRIM）
        reg1("TRIM") { Datum.Str(str(it).trim()) }
        reg1("LTRIM") { Datum.Str(str(it).trimStart()) }
        reg1("RTRIM") { Datum.Str(str(it).trimEnd()) }
        reg("TRIM", 2, 2) { a, _ -> Datum.Str(str(a[0]).trim(str(a[1]).singleOrNull() ?: ' ')) }
        // `TRIM(BOTH|LEADING|TRAILING 'x' FROM s)` 由解析器折成这个内部函数
        // （名字带下划线是刻意的：它不是 MySQL 的函数名，用户写不出来）
        reg("TRIM_SIDE", 3, 3) { a, _ ->
            val s0 = str(a[0])
            val cut = str(a[1]).ifEmpty { " " }
            Datum.Str(
                when (str(a[2]).uppercase()) {
                    "LEADING" -> s0.trimStart { it in cut }
                    "TRAILING" -> s0.trimEnd { it in cut }
                    else -> s0.trim { it in cut }
                },
            )
        }

        reg3("REPLACE") { s, from, to ->
            val f = str(from)
            // MySQL 里 `REPLACE('abc','','x')` 返回原串（不是无限插入）
            if (f.isEmpty()) Datum.Str(str(s)) else Datum.Str(str(s).replace(f, str(to)))
        }
        reg2("INSTR") { s, sub ->
            val i = str(s).indexOf(str(sub))
            Datum.I64(if (i < 0) 0 else (i + 1).toLong())
        }
        reg2("LOCATE") { sub, s -> locate(str(sub), str(s), 1L) }
        reg3("LOCATE") { sub, s, pos -> locate(str(sub), str(s), lng(pos)) }
        reg2("POSITION") { sub, s -> locate(str(sub), str(s), 1L) }
        reg2("STRCMP") { a, b -> Datum.I64(str(a).compareTo(str(b)).let { if (it < 0) -1 else if (it > 0) 1 else 0 }.toLong()) }

        reg3("LPAD") { s, n, pad -> pad(str(s), lng(n), str(pad), left = true) }
        reg3("RPAD") { s, n, pad -> pad(str(s), lng(n), str(pad), left = false) }

        reg("SUBSTRING_INDEX", 3, 3) { a, _ -> substringIndex(str(a[0]), str(a[1]), lng(a[2])) }

        reg1("ASCII") { Datum.I64(str(it).firstOrNull()?.code?.toLong() ?: 0L) }
        reg("CHAR", 1, Int.MAX_VALUE) { a, _ ->
            Datum.Str(a.joinToString("") { lng(it).toInt().toChar().toString() })
        }
        reg1("HEX") { d ->
            // HEX(字符串) 走字节；HEX(数字) 走 MySQL 的整数十六进制（含负数补码）
            if (d is Datum.Str || d is Datum.Bin) {
                Datum.Str(str(d).toByteArray(Charsets.UTF_8).joinToString("") { "%02X".format(it) })
            } else {
                Datum.Str(java.lang.Long.toHexString(lng(d)).uppercase())
            }
        }
        reg1("UNHEX") { d ->
            val t = str(d)
            if (t.length % 2 != 0) Datum.Null
            else Datum.Bin(
                ByteArray(t.length / 2) { i ->
                    t.substring(i * 2, i * 2 + 2).toIntOrNull(16)?.toByte()
                        ?: return@reg1 Datum.Null
                },
            )
        }
        reg1("MD5") { d -> Datum.Str(digest("MD5", str(d))) }
        reg1("SHA1") { d -> Datum.Str(digest("SHA-1", str(d))) }
        reg1("SHA2") { d -> Datum.Str(digest("SHA-256", str(d))) }
        reg2("SHA2") { d, bits ->
            val algo = when (lng(bits)) {
                0L, 256L -> "SHA-256"
                224L -> "SHA-224"
                384L -> "SHA-384"
                512L -> "SHA-512"
                else -> throw SqlException(
                    MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
                    "SHA2 只支持 224/256/384/512，实际 ${lng(bits)}",
                )
            }
            Datum.Str(digest(algo, str(d)))
        }
    }

    private const val MAX_REPEAT = 1_000_000L   // 防 `REPEAT('x', 1e9)` 把内存吃光

    private fun substring(s: String, pos: Long, lenArg: Datum?): Datum {
        // MySQL：pos 从 1 开始；负数表示从末尾倒数；越界返回空串
        val n = s.length.toLong()
        // pos = 0 是非法位置：MySQL 返回空串（不是"从开头算"）
        if (pos == 0L) return Datum.Str("")
        val start = when {
            pos > 0 -> pos - 1
            else -> n + pos
        }.coerceIn(0, n)
        val len = when {
            lenArg == null -> n - start
            else -> lng(lenArg).coerceAtLeast(0)
        }
        val end = (start + len).coerceAtMost(n)
        return Datum.Str(s.substring(start.toInt(), end.toInt()))
    }

    private fun locate(sub: String, s: String, from: Long): Datum {
        if (from <= 0) return Datum.I64(0)
        val i = s.indexOf(sub, (from - 1).coerceAtMost(s.length.toLong()).toInt())
        return Datum.I64(if (i < 0) 0 else (i + 1).toLong())
    }

    private fun pad(s: String, target: Long, padStr: String, left: Boolean): Datum {
        if (padStr.isEmpty() || target <= 0) return Datum.Str("")
        val t = target.coerceAtMost(MAX_REPEAT).toInt()
        if (s.length >= t) return Datum.Str(s.substring(0, t))
        val need = t - s.length
        val fill = (padStr.repeat(need / padStr.length + 1)).substring(0, need)
        return Datum.Str(if (left) fill + s else s + fill)
    }

    private fun substringIndex(s: String, delim: String, count: Long): Datum {
        if (delim.isEmpty() || count == 0L) return Datum.Str("")
        val parts = s.split(delim)
        return if (count > 0) {
            if (count >= parts.size) Datum.Str(s) else Datum.Str(parts.take(count.toInt()).joinToString(delim))
        } else {
            val k = (-count).toInt()
            if (k >= parts.size) Datum.Str(s) else Datum.Str(parts.takeLast(k).joinToString(delim))
        }
    }

    private fun digest(algo: String, text: String): String =
        java.security.MessageDigest.getInstance(algo)
            .digest(text.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    // ── 数值 ──────────────────────────────────────────────────────

    private fun registerNumeric() {
        reg1("ABS") { d ->
            when (d) {
                is Datum.I64 -> if (d.v == Long.MIN_VALUE) throw overflow("ABS") else Datum.I64(kotlin.math.abs(d.v))
                is Datum.Dec -> if (d.unscaled == Long.MIN_VALUE) throw overflow("ABS") else Datum.Dec(kotlin.math.abs(d.unscaled), d.scale)
                else -> Datum.F64(kotlin.math.abs(dbl(d)))
            }
        }
        reg1("SIGN") { d ->
            // MySQL 的 SIGN 返回 -1/0/1，且保留 DECIMAL 的写法（这里用整数，够用）
            val v = dbl(d)
            Datum.I64(when { v < 0 -> -1L; v > 0 -> 1L; else -> 0L })
        }
        reg1("CEIL", "CEILING") { d -> ceilFloor(d, up = true) }
        reg1("FLOOR") { d -> ceilFloor(d, up = false) }
        reg1("SQRT") { d ->
            val v = dbl(d)
            // MySQL：负数开方返回 NULL（不是报错）
            if (v < 0) Datum.Null else Datum.F64(kotlin.math.sqrt(v))
        }
        reg1("EXP") { Datum.F64(kotlin.math.exp(dbl(it))) }
        reg1("LN") { d -> val v = dbl(d); if (v <= 0) Datum.Null else Datum.F64(kotlin.math.ln(v)) }
        reg1("LOG2") { d -> val v = dbl(d); if (v <= 0) Datum.Null else Datum.F64(kotlin.math.log2(v)) }
        reg1("LOG10") { d -> val v = dbl(d); if (v <= 0) Datum.Null else Datum.F64(kotlin.math.log10(v)) }
        reg1("LOG") { d -> val v = dbl(d); if (v <= 0) Datum.Null else Datum.F64(kotlin.math.ln(v)) }
        reg2("LOG") { b, x ->
            val base = dbl(b); val v = dbl(x)
            if (base <= 0 || base == 1.0 || v <= 0) Datum.Null
            else Datum.F64(kotlin.math.ln(v) / kotlin.math.ln(base))
        }
        reg1("PI") { Datum.F64(Math.PI) }
        reg2("POW", "POWER") { a, b -> Datum.F64(Math.pow(dbl(a), dbl(b))) }
        reg2("MOD") { a, b ->
            // 两个定点数走定点取模（`lng` 会把小数截掉：MOD(2.5, 1) 曾经给 0 而不是 0.5）
            if (a is Datum.Dec && b is Datum.Dec) {
                val scale = maxOf(a.scale, b.scale)
                val au = a.unscaled * Datum.pow10Long(scale - a.scale)
                val bu = b.unscaled * Datum.pow10Long(scale - b.scale)
                if (bu == 0L) Datum.Null else Datum.Dec(au % bu, scale)
            } else {
                val d = lng(b)
                if (d == 0L) Datum.Null else Datum.I64(lng(a) % d)
            }
        }
        reg2("ATAN2") { a, b -> Datum.F64(kotlin.math.atan2(dbl(a), dbl(b))) }
        reg1("SIN") { Datum.F64(kotlin.math.sin(dbl(it))) }
        reg1("COS") { Datum.F64(kotlin.math.cos(dbl(it))) }
        reg1("TAN") { Datum.F64(kotlin.math.tan(dbl(it))) }
        reg1("DEGREES") { Datum.F64(Math.toDegrees(dbl(it))) }
        reg1("RADIANS") { Datum.F64(Math.toRadians(dbl(it))) }
        reg1("ASIN") { d -> val v = dbl(d); if (v < -1 || v > 1) Datum.Null else Datum.F64(kotlin.math.asin(v)) }
        reg1("ACOS") { d -> val v = dbl(d); if (v < -1 || v > 1) Datum.Null else Datum.F64(kotlin.math.acos(v)) }
        reg1("ATAN") { Datum.F64(kotlin.math.atan(dbl(it))) }

        reg1("ROUND") { d -> round(d, 0L) }
        reg2("ROUND") { d, n -> round(d, lng(n)) }
        reg1("TRUNCATE") { d -> truncate(d, 0L) }
        reg2("TRUNCATE") { d, n -> truncate(d, lng(n)) }
    }

    private fun overflow(fn: String) = SqlException(
        MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003", "BIGINT value is out of range in '$fn(...)'",
    )

    private fun ceilFloor(d: Datum, up: Boolean): Datum = when (d) {
        // 整数没什么可取的
        is Datum.I64 -> d
        is Datum.Bool -> Datum.I64(if (d.v) 1 else 0)
        // ⚠️ 定点数**必须真的取整**：曾经 `isExact(d) -> d` 把它原样返回，
        // 于是 `CEILING(2.50)` 在 DECIMAL 列上给 2.50（而不是 3），
        // 而同样的小数写成字面量走浮点分支又是对的——同一个函数两种行为
        is Datum.Dec -> {
            val pow = Datum.pow10Long(d.scale)
            val q = d.unscaled / pow
            val rem = d.unscaled % pow
            val bump = if (rem != 0L && (up == (d.unscaled > 0))) 1L else 0L
            Datum.Dec((q + bump) * pow, d.scale)
        }
        else -> {
            val v = dbl(d)
            Datum.F64(if (up) kotlin.math.ceil(v) else kotlin.math.floor(v))
        }
    }

    /**
     * `ROUND(x, d)`：**四舍五入远离零**（MySQL 的规则），不是 Kotlin 的
     * `roundToLong`（银行家舍入，`.5` 会往偶数靠）。
     *
     * 定点输入走整数运算保持精确；浮点输入走 `BigDecimal.valueOf`
     * （它按 `Double.toString` 解释，`2.675` 就是 2.675，而不是二进制近似的 2.67499999…）。
     */
    private fun round(d: Datum, digits: Long): Datum {
        val d0 = digits.coerceIn(-30, 30).toInt()
        return when (d) {
            is Datum.I64 -> if (d0 >= 0) Datum.Dec(d.v * Datum.pow10Long(d0), d0) else truncate(d, digits)
            is Datum.Dec -> {
                val unscaled = rescaleHalfUp(d.unscaled, d.scale, d0)
                Datum.Dec(unscaled, d0)
            }
            else -> {
                val bd = BigDecimal.valueOf(dbl(d)).setScale(d0, RoundingMode.HALF_UP)
                Datum.F64(bd.toDouble())
            }
        }
    }

    private fun truncate(d: Datum, digits: Long): Datum {
        val d0 = digits.coerceIn(-30, 30).toInt()
        return when (d) {
            is Datum.I64 -> if (d0 >= 0) Datum.Dec(d.v * Datum.pow10Long(d0), d0) else Datum.I64(d.v / Datum.pow10Long(-d0))
            is Datum.Dec -> Datum.Dec(rescaleTowardZero(d.unscaled, d.scale, d0), d0)
            else -> {
                val bd = BigDecimal.valueOf(dbl(d)).setScale(d0, RoundingMode.DOWN)
                Datum.F64(bd.toDouble())
            }
        }
    }

    /** 标度变换：目标标度更大时精确放大，更小时按 HALF_UP（远离零）舍入。 */
    private fun rescaleHalfUp(unscaled: Long, from: Int, to: Int): Long {
        if (to >= from) return unscaled * Datum.pow10Long(to - from)
        val div = Datum.pow10Long(from - to)
        val q = unscaled / div
        val rem = unscaled % div
        return if (kotlin.math.abs(rem) * 2 >= div) q + (if (unscaled < 0) -1 else 1) else q
    }

    /** 同上，但向零截断（`TRUNCATE`）。 */
    private fun rescaleTowardZero(unscaled: Long, from: Int, to: Int): Long {
        if (to >= from) return unscaled * Datum.pow10Long(to - from)
        return unscaled / Datum.pow10Long(from - to)
    }

    // ── 日期时间 ──────────────────────────────────────────────────

    private fun registerDateTime() {
        reg("NOW", 0, 1) { _, ctx -> Datum.Ts(ctx.nowMillis) }
        reg("SYSDATE", 0, 1) { _, _ -> Datum.Ts(System.currentTimeMillis()) }
        reg("CURRENT_TIMESTAMP", 0, 1) { _, ctx -> Datum.Ts(ctx.nowMillis) }
        reg("LOCALTIME", 0, 1) { _, ctx -> Datum.Ts(ctx.nowMillis) }
        reg("LOCALTIMESTAMP", 0, 1) { _, ctx -> Datum.Ts(ctx.nowMillis) }
        reg("UTC_TIMESTAMP", 0, 1) { _, ctx -> Datum.Ts(ctx.nowMillis) }
        reg("CURDATE", 0, 0) { _, ctx -> Datum.Ts(startOfDay(ctx.nowMillis)) }
        reg("CURRENT_DATE", 0, 0) { _, ctx -> Datum.Ts(startOfDay(ctx.nowMillis)) }
        reg("UTC_DATE", 0, 0) { _, ctx -> Datum.Ts(startOfDay(ctx.nowMillis)) }
        reg("CURTIME", 0, 1) { _, ctx -> Datum.Str(Datum.formatDateTime(ctx.nowMillis).substringAfter(' ')) }
        reg("CURRENT_TIME", 0, 1) { _, ctx -> Datum.Str(Datum.formatDateTime(ctx.nowMillis).substringAfter(' ')) }
        reg("UNIX_TIMESTAMP", 0, 1) { a, ctx ->
            if (a.isEmpty()) Datum.I64(ctx.nowMillis / 1000) else Datum.I64(asMillis(a[0]) / 1000)
        }
        reg("FROM_UNIXTIME", 1, 2) { a, _ ->
            val ms = lng(a[0]) * 1000
            if (a.size == 2) Datum.Str(formatWith(ms, str(a[1]))) else Datum.Ts(ms)
        }

        reg1("DATE") { Datum.Ts(startOfDay(asMillis(it))) }
        reg1("TIME") { Datum.Str(Datum.formatDateTime(asMillis(it)).substringAfter(' ')) }
        reg1("YEAR") { Datum.I64(utc(asMillis(it)).year.toLong()) }
        reg1("MONTH") { Datum.I64(utc(asMillis(it)).monthValue.toLong()) }
        reg1("DAY", "DAYOFMONTH") { Datum.I64(utc(asMillis(it)).dayOfMonth.toLong()) }
        reg1("HOUR") { Datum.I64(utc(asMillis(it)).hour.toLong()) }
        reg1("MINUTE") { Datum.I64(utc(asMillis(it)).minute.toLong()) }
        reg1("SECOND") { Datum.I64(utc(asMillis(it)).second.toLong()) }
        reg1("MICROSECOND") { Datum.I64((asMillis(it) % 1000) * 1000) }
        reg1("QUARTER") { Datum.I64(((utc(asMillis(it)).monthValue - 1) / 3 + 1).toLong()) }
        reg1("DAYOFYEAR") { Datum.I64(utc(asMillis(it)).dayOfYear.toLong()) }
        // MySQL：DAYOFWEEK 1=周日 … 7=周六；WEEKDAY 0=周一 … 6=周日
        reg1("DAYOFWEEK") { Datum.I64((utc(asMillis(it)).dayOfWeek.value % 7 + 1).toLong()) }
        reg1("WEEKDAY") { Datum.I64((utc(asMillis(it)).dayOfWeek.value - 1).toLong()) }
        reg1("WEEKOFYEAR") { Datum.I64(utc(asMillis(it)).get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear()).toLong()) }
        reg1("MONTHNAME") { Datum.Str(utc(asMillis(it)).month.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH)) }
        reg1("DAYNAME") { Datum.Str(utc(asMillis(it)).dayOfWeek.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH)) }
        reg1("LAST_DAY") { d ->
            val date = utc(asMillis(d)).toLocalDate()
            Datum.Ts(date.withDayOfMonth(date.lengthOfMonth()).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli())
        }
        reg1("TO_DAYS") { Datum.I64(asMillis(it) / 86_400_000L + 719_528L) }

        reg2("DATEDIFF") { a, b ->
            Datum.I64(startOfDay(asMillis(a)) / 86_400_000L - startOfDay(asMillis(b)) / 86_400_000L)
        }
        reg2("DATE_FORMAT") { d, f -> Datum.Str(formatWith(asMillis(d), str(f))) }
        reg2("TIME_FORMAT") { d, f -> Datum.Str(formatWith(asMillis(d), str(f))) }
        reg2("STR_TO_DATE") { s, f -> parseWith(str(s), str(f))?.let { Datum.Ts(it) } ?: Datum.Null }
        reg3("TIMESTAMPDIFF") { unit, a, b ->
            Datum.I64(timestampDiff(str(unit), asMillis(a), asMillis(b)))
        }
        reg3("TIMESTAMPADD") { unit, n, d ->
            Datum.Ts(addInterval(asMillis(d), lng(n), str(unit)))
        }
        reg2("ADDDATE") { d, n -> Datum.Ts(addInterval(asMillis(d), lng(n), "DAY")) }
        reg3("ADDDATE") { d, n, unit -> Datum.Ts(addInterval(asMillis(d), lng(n), str(unit))) }
        reg2("SUBDATE") { d, n -> Datum.Ts(addInterval(asMillis(d), -lng(n), "DAY")) }
        reg3("SUBDATE") { d, n, unit -> Datum.Ts(addInterval(asMillis(d), -lng(n), str(unit))) }
        reg2("DATE_ADD") { d, n -> Datum.Ts(addInterval(asMillis(d), lng(n), "DAY")) }
        reg2("DATE_SUB") { d, n -> Datum.Ts(addInterval(asMillis(d), -lng(n), "DAY")) }
        // INTERVAL 形式由解析器折成三参调用：DATE_ADD(d, n, UNIT)
        reg3("DATE_ADD") { d, n, unit -> Datum.Ts(addInterval(asMillis(d), lng(n), str(unit).uppercase())) }
        reg3("DATE_SUB") { d, n, unit -> Datum.Ts(addInterval(asMillis(d), -lng(n), str(unit).uppercase())) }
    }

    /** 时间函数统一入口：把参数解成毫秒（字符串走**唯一**那个 UTC 解析器）。 */
    private fun asMillis(d: Datum): Long = when (d) {
        is Datum.Ts -> d.millis
        is Datum.I64 -> d.v
        is Datum.Str -> Datum.parseDateTime(d.v) ?: throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007",
            "无法把 '${d.v}' 解释为时间（期望 'yyyy-MM-dd[ HH:mm:ss[.SSS]]'，按 UTC）",
        )
        is Datum.Dec -> d.unscaled / Datum.pow10Long(d.scale.coerceAtMost(18))
        else -> throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "无法把 $d 解释为时间")
    }

    private fun utc(millis: Long) = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC)

    private fun startOfDay(millis: Long): Long =
        utc(millis).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()

    private fun addInterval(millis: Long, n: Long, unit: String): Long {
        val dt = utc(millis)
        return when (unit.uppercase()) {
            "MICROSECOND" -> millis + n / 1000
            "SECOND" -> millis + n * 1000
            "MINUTE" -> millis + n * 60_000
            "HOUR" -> millis + n * 3_600_000
            "DAY" -> millis + n * 86_400_000L
            "WEEK" -> millis + n * 7 * 86_400_000L
            "MONTH" -> dt.plusMonths(n).toInstant().toEpochMilli()
            "QUARTER" -> dt.plusMonths(n * 3).toInstant().toEpochMilli()
            "YEAR" -> dt.plusYears(n).toInstant().toEpochMilli()
            else -> throw SqlException(
                MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
                "不支持的时间单位 '$unit'（支持 MICROSECOND/SECOND/MINUTE/HOUR/DAY/WEEK/MONTH/QUARTER/YEAR）",
            )
        }
    }

    private fun timestampDiff(unit: String, a: Long, b: Long): Long {
        val from = utc(a); val to = utc(b)
        return when (unit.uppercase()) {
            "MICROSECOND" -> (b - a) * 1000
            "SECOND" -> (b - a) / 1000
            "MINUTE" -> (b - a) / 60_000
            "HOUR" -> (b - a) / 3_600_000
            "DAY" -> startOfDay(b) / 86_400_000L - startOfDay(a) / 86_400_000L
            "WEEK" -> (startOfDay(b) / 86_400_000L - startOfDay(a) / 86_400_000L) / 7
            "MONTH" -> ((to.year - from.year) * 12 + (to.monthValue - from.monthValue)).toLong()
            "QUARTER" -> (((to.year - from.year) * 12 + (to.monthValue - from.monthValue)) / 3).toLong()
            "YEAR" -> (to.year - from.year).toLong()
            else -> throw SqlException(
                MySqlErrors.ER_WRONG_ARGUMENTS, "HY000", "不支持的时间单位 '$unit'",
            )
        }
    }

    // ── 格式化（DATE_FORMAT / STR_TO_DATE / FROM_UNIXTIME）─────────

    /**
     * MySQL 的 `DATE_FORMAT` 格式符。
     *
     * 支持日常会用到的那些；不认识的格式符**原样输出**（MySQL 也是这么做的），
     * 而不是报错——`DATE_FORMAT(d, '%Y年%m月')` 里的中文必须能过。
     */
    internal fun formatWith(millis: Long, fmt: String): String {
        val dt = utc(millis)
        val sb = StringBuilder()
        var i = 0
        while (i < fmt.length) {
            val c = fmt[i]
            if (c != '%' || i == fmt.length - 1) { sb.append(c); i++; continue }
            val spec = fmt[i + 1]
            i += 2
            sb.append(
                when (spec) {
                    'Y' -> "%04d".format(dt.year)
                    'y' -> "%02d".format(dt.year % 100)
                    'm' -> "%02d".format(dt.monthValue)
                    'c' -> dt.monthValue.toString()
                    'M' -> dt.month.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH)
                    'b' -> dt.month.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH)
                    'd' -> "%02d".format(dt.dayOfMonth)
                    'e' -> dt.dayOfMonth.toString()
                    'D' -> ordinal(dt.dayOfMonth)
                    'H' -> "%02d".format(dt.hour)
                    'k' -> dt.hour.toString()
                    'h', 'I' -> "%02d".format(if (dt.hour % 12 == 0) 12 else dt.hour % 12)
                    'l' -> (if (dt.hour % 12 == 0) 12 else dt.hour % 12).toString()
                    'i' -> "%02d".format(dt.minute)
                    's', 'S' -> "%02d".format(dt.second)
                    'f' -> "%06d".format(((millis % 1000) + 1000) % 1000 * 1000)
                    'p' -> if (dt.hour < 12) "AM" else "PM"
                    'j' -> "%03d".format(dt.dayOfYear)
                    'W' -> dt.dayOfWeek.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH)
                    'a' -> dt.dayOfWeek.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH)
                    'w' -> (dt.dayOfWeek.value % 7).toString()
                    'T' -> "%02d:%02d:%02d".format(dt.hour, dt.minute, dt.second)
                    'r' -> "%02d:%02d:%02d %s".format(
                        if (dt.hour % 12 == 0) 12 else dt.hour % 12, dt.minute, dt.second,
                        if (dt.hour < 12) "AM" else "PM",
                    )
                    'U' -> "%02d".format(dt.get(java.time.temporal.WeekFields.SUNDAY_START.weekOfYear()))
                    'u' -> "%02d".format(dt.get(java.time.temporal.WeekFields.ISO.weekOfYear()))
                    'V' -> "%02d".format(dt.get(java.time.temporal.WeekFields.SUNDAY_START.weekOfWeekBasedYear()))
                    'v' -> "%02d".format(dt.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear()))
                    'X' -> "%04d".format(dt.get(java.time.temporal.WeekFields.SUNDAY_START.weekBasedYear()))
                    'x' -> "%04d".format(dt.get(java.time.temporal.WeekFields.ISO.weekBasedYear()))
                    '%' -> "%"
                    else -> "%$spec"          // 不认识的格式符原样输出（MySQL 同）
                },
            )
        }
        return sb.toString()
    }

    private fun ordinal(n: Int): String {
        val suffix = if (n % 100 in 11..13) "th" else when (n % 10) {
            1 -> "st"; 2 -> "nd"; 3 -> "rd"; else -> "th"
        }
        return "$n$suffix"
    }

    /**
     * `STR_TO_DATE`：把格式串转成一个 `java.time` 解析器。
     *
     * 与 [formatWith] 共用一个格式符表——两边不共享的话，
     * `DATE_FORMAT` 能写出来的格式 `STR_TO_DATE` 解不出来，而用户是先存后取的。
     */
    internal fun parseWith(text: String, fmt: String): Long? {
        val pattern = StringBuilder()
        var i = 0
        while (i < fmt.length) {
            val c = fmt[i]
            if (c != '%' || i == fmt.length - 1) { pattern.append(if (c.isLetter()) "'$c'" else c); i++; continue }
            val spec = fmt[i + 1]
            i += 2
            pattern.append(
                when (spec) {
                    'Y' -> "uuuu"; 'y' -> "uu"; 'm' -> "MM"; 'c' -> "M"; 'M' -> "MMMM"; 'b' -> "MMM"
                    'd' -> "dd"; 'e' -> "d"; 'H', 'k' -> "HH"; 'h', 'I', 'l' -> "hh"; 'i' -> "mm"
                    's', 'S' -> "ss"; 'f' -> "SSSSSS"; 'p' -> "a"; 'j' -> "DDD"
                    'T' -> "HH:mm:ss"; 'r' -> "hh:mm:ss a"
                    '%' -> "'%'"
                    else -> return null
                },
            )
        }
        return try {
            val fmtObj = java.time.format.DateTimeFormatter.ofPattern(pattern.toString(), java.util.Locale.ENGLISH)
            // 逐级尝试：完整日期时间 → 只有日期 → 只有时间（时间补 1970-01-01）
            runCatching {
                java.time.LocalDateTime.parse(text, fmtObj).toInstant(ZoneOffset.UTC).toEpochMilli()
            }.recoverCatching {
                java.time.LocalDate.parse(text, fmtObj).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
            }.recoverCatching {
                java.time.LocalTime.parse(text, fmtObj)
                    .atDate(java.time.LocalDate.of(1970, 1, 1)).toInstant(ZoneOffset.UTC).toEpochMilli()
            }.getOrNull()
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    // ── 类型转换（CAST / CONVERT）──────────────────────────────────

    private fun registerCasts() {
        // CAST 的语法形态由解析器识别（`CAST(x AS type)`），这里只暴露一个内部入口
        // 供求值器调用；`CONVERT(x, type)` 是普通函数形态
        reg2("CONVERT") { v, t -> castTo(v, str(t)) }
    }

    /**
     * `CAST(x AS t)`。
     *
     * ## 与 MySQL 的差异（刻意的）
     *
     * MySQL 在宽松模式下把转换失败变成 0 加一条 warning（`CAST('abc' AS SIGNED)` = 0）。
     * 本系统没有 warning 通道，给 0 就是**静默的错误答案**——所以这里抛 1292。
     * 严格模式下的 MySQL 同样是报错，两边在"严谨"这一点上是对齐的。
     */
    internal fun castTo(v: Datum, typeName: String): Datum {
        if (v is Datum.Null) return Datum.Null
        // 类型名可能带精度：`DECIMAL(10,2)`、`CHAR(8)`。参数部分单独拿出来，
        // 主名用于匹配分支——不剥掉的话 `when (t)` 一个分支都命中不了，
        // 报"不支持的目标类型 'DECIMAL(10,2)'"（而 DECIMAL 明明是支持的）
        val raw = typeName.uppercase().trim()
        val base = raw.substringBefore('(').trim()
        val args = raw.substringAfter('(', "").substringBefore(')').split(',').mapNotNull { it.trim().toIntOrNull() }
        val t = base.removePrefix("SIGNED ").removePrefix("UNSIGNED ").trim()
        return when (t) {
            "SIGNED", "SIGNED INTEGER", "INTEGER", "INT", "BIGINT" -> Datum.I64(lng(v))
            "UNSIGNED", "UNSIGNED INTEGER" -> {
                val l = lng(v)
                if (l < 0) throw SqlException(
                    MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003",
                    "CAST AS UNSIGNED 不接受负数（$l）",
                )
                Datum.I64(l)
            }
            "DOUBLE", "FLOAT", "REAL" -> Datum.F64(dbl(v))
            "DECIMAL", "NUMERIC", "FIXED" -> {
                val scale = args.getOrNull(1) ?: 0
                val unscaled = java.math.BigDecimal.valueOf(dbl(v))
                    .setScale(scale, java.math.RoundingMode.HALF_UP)
                    .movePointRight(scale)
                Datum.Dec(unscaled.toLong(), scale)
            }
            "CHAR", "NCHAR", "VARCHAR", "TEXT" -> Datum.Str(str(v))
            "BINARY", "VARBINARY", "BLOB" -> Datum.Bin(str(v).toByteArray(Charsets.UTF_8))
            "DATE" -> Datum.Ts(startOfDay(asMillis(v)))
            "DATETIME", "TIMESTAMP" -> Datum.Ts(asMillis(v))
            "TIME" -> Datum.Str(Datum.formatDateTime(asMillis(v)).substringAfter(' '))
            "JSON" -> {
                val text = str(v).trim()
                if (!JsonFunctions.isValid(Datum.Str(text)).asBoolean()) {
                    throw SqlException(
                        MySqlErrors.ER_INVALID_JSON_TEXT, "22032",
                        "Invalid JSON text in argument 1 to function cast_as_json: \"$text\"",
                    )
                }
                Datum.Str(text)
            }
            else -> throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "CAST 不支持的目标类型 '$typeName'",
            )
        }
    }
}
