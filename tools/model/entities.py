from tortoise import fields
from tortoise.models import Model

class BaseEntity(Model):
    id = fields.IntField(pk=True, description="主键")
    create_time = fields.DatetimeField(auto_now_add=True, description='创建时间', null=True)
    update_time = fields.DatetimeField(auto_now=True, description='更新时间', null=True)

    # order,eg:"-update_time" ，按更新时间倒序排
    @classmethod
    async def page_list(cls, page_num: int, page_size: int, order_by: str, **query):
        if order_by:
            result = await cls.filter(**query).order_by(order_by).offset((page_num - 1) * page_size).limit(
                page_size)
        else:
            result = await cls.filter(**query).offset((page_num - 1) * page_size).limit(page_size)
        count = await cls.filter(**query).count()
        return count, list(result)

class AppGroup(Model):
    id = fields.IntField(pk=True, description="主键")
    app_id = fields.BigIntField(description="协议号的tgid")
    url = fields.TextField(description="url")

    class Meta:
        table = "tmp_listen"
        table_description = "协议群关联表"