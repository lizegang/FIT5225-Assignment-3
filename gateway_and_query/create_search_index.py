#此文件只需被执行一次用于生成多层索引，目前已经被执行，请勿执行

from tablestore import SearchIndexMeta, FieldSchema, FieldType, IndexSetting
from fc_query_handler.table_store_client import get_table_client

def create_tags_index(client, table_name="bird_media_meta", index_name="bird_media_index"):
    # 定义索引字段
    fields = [
        FieldSchema('file_id', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),
        FieldSchema('timestamp', FieldType.LONG, index=True, enable_sort_and_agg=True, store=True),

        FieldSchema('user_id', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),
        FieldSchema('file_type', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),
        FieldSchema('thumbnail_url', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),
        FieldSchema('oss_url', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),

        # tags 拆分字段
        FieldSchema('species', FieldType.KEYWORD, index=True, enable_sort_and_agg=False, store=True),
        FieldSchema('count', FieldType.LONG, index=True, enable_sort_and_agg=True, store=True),

        # 原始 tags 字段全文索引
        FieldSchema('tags', FieldType.TEXT, index=True, enable_sort_and_agg=False, store=True),
    ]

    # 索引设置（可指定路由字段）
    index_setting = IndexSetting(routing_fields=['file_id'])

    # 创建 SearchIndexMeta
    index_meta = SearchIndexMeta(
        fields=fields,
        index_setting=index_setting,
        index_sort=None  # 如果没有需要排序的字段，可以为 None
    )

    # 创建索引
    client.create_search_index(table_name=table_name, index_name=index_name, index_meta=index_meta)
    print(f"Search index '{index_name}' created on table '{table_name}'.")

if __name__ == "__main__":
    client = get_table_client()
    create_tags_index(client)
