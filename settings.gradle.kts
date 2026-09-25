rootProject.name = "yang-base"

// 模块划分对应设计文档 §15.3，但为降低构建复杂度做了合并：
//   yb-storage  吸收原 yb-catalog / yb-meta-* / yb-coordination-api
//   yb-index    吸收原 yb-index-{api,ordered,skip,fulltext}
//   yb-sql      吸收原 yb-sql-{parser,planner,exec}
// 模块边界仍按设计文档的职责划分，只是不再一一对应独立 artifact。
include(
    "yb-common",
    "yb-objectstore",
    "yb-storage",
    "yb-txn",
    "yb-ms",
    "yb-index",
    "yb-sql",
    "yb-mysql",
    "yb-server",
    "yb-bench",
    "yb-cli",
)
