"""Infrastructure Layer — 持久化、调度器、可观测性与安全。

子包：db / repositories、scheduler、calendar、security、observability。
无向量库。

可以 import：Domain Port（实现接口）。
禁止 import：Application、API。Domain 不直接 import 本层。
"""
