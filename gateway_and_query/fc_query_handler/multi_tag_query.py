import json
from tablestore import BoolQuery, RangeQuery, TermQuery, SearchQuery, Sort, FieldSort, SortOrder, ColumnReturnType, ColumnsToGet
from table_store_client import get_table_client

def simplify_rows(rows):
    result = []
    for header, fields in rows:
        record = {}
        # 加入 header 部分
        for k, v in header:
            record[k] = v
        # 加入 fields 部分（忽略最后的 timestamp）
        for field in fields:
            k, v, *_ = field  # 取前两个
            record[k] = v
        result.append(record)
    return result

def multi_condition_query(client = None,
                          table_name = "bird_media_meta",
                          index_name = 'bird_media_index',
                          count_min = 1,
                          count_max = 2,
                          species = None,
                          minimum_should_match = 1,
                          species_not = None,
                          original_output = False,
                          ):
    """
    执行带有多条件逻辑的搜索查询（must、must_not、should），并返回查询结果。

    该函数基于 TableStore SearchIndex 的 BoolQuery 构建搜索逻辑，支持数值范围过滤、
    包含条件（should）、排除条件（must_not），并可选择返回原始结果或简化后的 JSON 格式。

    参数:
        client: TableStore 客户端实例。
        table_name (str, 可选): 数据表名称，默认 "bird_media_meta"。
        index_name (str, 可选): 索引名称，默认 "bird_media_index"。
        count_min (int, 可选): count 字段的最小值（闭区间），默认 1。
        count_max (int, 可选): count 字段的最大值（闭区间），默认 2。
        species (list[str], 必填): 需要匹配的 species 值列表，用于 should 查询。
        minimum_should_match (int, 可选): should 查询至少满足的条件数量，默认 1。
        species_not (list[str], 可选): 需要排除的 species 值列表，用于 must_not 查询。
        original_output (bool, 可选): 是否返回原始 rows 结构，默认 False。
            - False: 对结果进行简化，剔除冗余时间戳，返回扁平化 JSON。
            - True: 保留原始嵌套结构。

    返回:
        SearchResponse: 包含 request_id、是否成功、total_count 及 rows（原始或简化）。
    """
    if client is None:
        client = get_table_client()
    if species is None:
        species = []
    if species_not is None:
        species_not = []
    bool_query = BoolQuery(
        must_queries=[
            RangeQuery('count', range_from=count_min, include_lower=True),
            RangeQuery('count', range_to=count_max, include_upper=True)
        ],
        # 设置需要排除的子查询条件。
        must_not_queries=[
            TermQuery('species', s) for s in species_not
        ],
        should_queries={
            TermQuery('species', s) for s in species
        },
        minimum_should_match = minimum_should_match
    )
    # 构造完整查询语句，包括排序的列，返回前100行以及返回查询结果总的行数。
    search_response = client.search(
        table_name,
        index_name,
        SearchQuery(
            bool_query,
            sort=Sort(sorters=[FieldSort('count', SortOrder.ASC)]),
            limit=100,
            get_total_count=True),
        ColumnsToGet(return_type=ColumnReturnType.ALL)
    )
    print('request_id : %s' % search_response.request_id)
    print('is_all_succeed : %s' % search_response.is_all_succeed)
    print('total_count : %s' % search_response.total_count)
    if original_output:
        print('rows : %s' % search_response.rows)
    else:
        search_response.rows = simplify_rows(search_response.rows)
        print('rows : %s' % search_response.rows)
    return search_response.rows



if __name__ == "__main__":
    multi_condition_query(species=["sparrow","crow"], species_not=[])


