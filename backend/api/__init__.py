"""API Layer — HTTP/SSE 路由、鉴权、参数校验、响应格式。

可以 import:
  - Application 层的 Command、Query、DTO
  - Domain 层的枚举、异常
禁止 import:
  - Domain 层的 Entity/ValueObject（只能用 DTO）
  - Infrastructure 层
"""
