from  fc_query_handler.multi_tag_query import multi_condition_query
def format_response():
    return multi_condition_query(minimum_should_match = 0,count_min = 0, count_max = 2e9)

if __name__ == '__main__':
    output = format_response()
    print(type(output))
    print(len(output))