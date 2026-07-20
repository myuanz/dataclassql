# index

DataclassQL 是一个基于 **平凡 dataclass** 的 ORM 生成器, 可生成类型提示完整精巧的数据库客户端. 

模型文件保持干净、直观, 无需起手加一堆导入, 也没有 `mapped_column()`、`Annotation` 或额外的基类继承, 大部分时候只需要 `@dataclass`

---

## 设计目标

* **静态类型友好**: 全程可获得完美的补全体验。本库作为 [prisma client python](https://prisma-client-py.readthedocs.io/en/stable/) 的精神继承者, 致力于完成如下体验: 

![prisma client python 示例](https://prisma-client-py.readthedocs.io/en/stable/showcase.gif)


* **最小语法负担**: 模型定义是平凡的 Python dataclass, Python 本身就是 DSL
* **约定大于配置**: 常用定义只需写少量代码
* **零成本抽象**: 工具生成的数据库转换代码与手写版本有相同的速度