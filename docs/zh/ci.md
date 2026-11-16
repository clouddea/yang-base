---
---

# 持续集成

[English](../en/ci.md) | [简体中文](ci.md)

`.github/workflows/` 下有 5 个 workflow。其中 4 个完全不需要凭据；第 5 个要访问真实的云对象存储，因此需要一个**专门收窄过权限**的账号。

| Workflow | 触发方式 | 凭据 | 它能证明什么 |
| --- | --- | --- | --- |
| `ci.yml` | push、pull request | 无 | 构建、单元 + 集成测试、覆盖率门禁 |
| `bench.yml` | pull request、手动 | 无 | 针对仓内后端的延迟 / 请求数基准 |
| `docker.yml` | 手动、发版 | Docker Hub token | 发布的镜像能启动、能应答 SQL、能正常关闭 |
| `release.yml` | tag `v*` | Docker Hub token | 发版预检、产物、Release 正文 |
| `oss.yml` | 每晚、手动 | **阿里云 OSS AccessKey** | 同一套一致性用例打在真实云存储上 |

`ci.yml` 跑在 `pull_request` 上，**包含来自 fork 的 PR**，所以它永远不能拿到云凭据。真实后端检查因此单独放在 `oss.yml` 里，且不在 `pull_request` 上触发 —— 这是一条**结构上的**边界，而不是靠人小心。

## OSS 一致性（`oss.yml`）

### 为什么是每晚一次，而不是每次提交

每个用例类在 `setUp` 里都会清空自己的桶。GitHub runner 到杭州桶的单次往返 100–300 ms，而清空一次是「LIST + 每个对象一次 DELETE（DELETE 本身还带一次 HEAD）」。每次 push 都跑意味着真金白银的账单、网络抖动带来的假红，以及一个**对你的改动毫无信息量**的红灯。每晚一次足够回答这个 workflow 唯一想问的问题：*代码还跟真实云对得上吗？*

### 五个 secret

| Secret | 含义 |
| --- | --- |
| `OSS_ENDPOINT` | 例如 `oss-cn-hangzhou.aliyuncs.com` |
| `OSS_BUCKET` | CI 桶的前缀，各 job 自己加后缀 |
| `OSS_REGION` | 例如 `cn-hangzhou` |
| `OSS_ACCESS_KEY_ID` | **CI RAM 子账号**的 AccessKey |
| `OSS_ACCESS_KEY_SECRET` | 它的 secret |

各 job **从不共用桶**：一致性 job 用 `${OSS_BUCKET}-conformance`，每个集成测试分片用 `${OSS_BUCKET}-<group>`。共用桶不是「慢一点」—— 每个类都会清空整桶，两个 runner 共用一个桶就是**在测试运行期间互相删掉对方正在读的数据**，报出来的 `ObjectNotFoundException` 看起来像一致性套件的缺陷，其实是调度问题。桶名里必须含 `-ci`，守卫步骤发现不含就直接拒绝继续 —— 这条刹车很粗糙，但它确实能挡住「secret 配错指到一个有数据的桶」然后把桶清空。

### 为什么必须用子账号而不是主账号 AccessKey

这套用例会建桶、删桶、无脑清空桶。拿主账号 AccessKey 去跑，等于一次桶名写错就能毁掉整个账号。对应 issue #25。子账号只需要这些权限：

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["oss:ListBuckets"],
      "Resource": ["acs:oss:*:*:*"]
    },
    {
      "Effect": "Allow",
      "Action": ["oss:*"],
      "Resource": [
        "acs:oss:*:*:yangbase-ci*",
        "acs:oss:*:*:yangbase-ci*/*"
      ]
    }
  ]
}
```

阿里云 IAM 里 `oss:ListBuckets` 无法按桶前缀收窄，它是唯一不得不保留的账号级权限。其余全部被 `yangbase-ci*` 前缀框住。

密钥要定期轮换。因为这个 key 只有 `oss.yml` 在用，轮换是单点操作：改 secret、手动触发一次 workflow、确认绿。

### 预检

两个 job 的第一步都是 `scripts/check-oss-secrets.sh`：检查五个变量非空，缺了就直接失败，并列出缺哪个、去哪配。没有它的时候，少了 secret 会在几分钟后从对象存储 SPI 深处冒出 `SignatureDoesNotMatch` 或 `invalid credentials` —— 这条信息指向代码而不是配置，历史上不止一次被误判。

脚本只打印变量名，从不打印值。本地可以按 CI 的方式跑：

```bash
OSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com \
OSS_BUCKET=yangbase-ci \
OSS_REGION=cn-hangzhou \
OSS_ACCESS_KEY_ID=x \
OSS_ACCESS_KEY_SECRET=y \
./scripts/check-oss-secrets.sh
```

### 手动跑

只想验证凭据和一致性套件时，用 `workflow_dispatch` 加 `skip_integration: true` —— 五个分片的集成测试是贵的那一半。
