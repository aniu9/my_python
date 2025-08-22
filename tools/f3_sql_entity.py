import json
import re

from tools.util import file_util as fileUtil

def snake_to_camel(snake_str):
    return re.sub(r'_([a-z])', lambda m: m.group(1).upper(), snake_str)

def snake_to_big_camel(snake_str):
    return ''.join(x.title() for x in snake_str)

def remove_comment_spaces(sql_field_definition):
    """
    处理SQL字段定义，移除注释部分的所有空格

    参数:
        sql_field_definition: SQL字段定义字符串
        例如: "field_type VARCHAR(50) COMMENT '字段类型（string, number, date）'"

    返回:
        处理后的字符串
        例如: "field_type VARCHAR(50) COMMENT '字段类型（string,number,date）'"
    """
    # 正则表达式匹配 COMMENT 后面的字符串
    pattern = r"(COMMENT\s+['\"])(.*?)(['\"])"

    def remove_spaces(match):
        # 提取注释内容并移除所有空格
        prefix = match.group(1)  # 保留 COMMENT 和引号部分
        content = match.group(2).replace(" ", "")
        suffix = match.group(3)  # 保留结束引号
        return f"{prefix}{content}{suffix}"

    # 使用正则替换
    return re.sub(pattern, remove_spaces, sql_field_definition, flags=re.IGNORECASE)

def sql_to_entity():
    lstSource = fileUtil.read_txt_to_list(fileUtil.get_run_path_file('f0_source.txt'))

    lstResult = []
    for item in lstSource:
        if 'PRIMARY' in item or "create_time" in item or "update_time" in item or "unique" in item or "COMMENT=" in item:
            continue

        # 提取表名
        if 'CREATE TABLE' in item:
            match = re.search(r'CREATE TABLE if not exists (\w+)\s*\(?', item, re.IGNORECASE)
            if not match:
                raise ValueError("Invalid SQL format. Could not find table name.")
            table_name = match.group(1)
            words = table_name.split('_')
            words.pop(0)
            class_name = snake_to_big_camel(words)
            sclass = f'''
@Data
@TableName("{table_name}")
public class {class_name} extends BaseEntity {{
'''
            lstResult.append(sclass)
            continue

        # 提取字段
        item = remove_comment_spaces(item)
        arr = re.sub(r'\s+', ' ', item.strip()).split(" ")
        data_type = arr[1].lower()
        date_format=''
        if 'varchar' in data_type:
            data_type = "String"
        elif 'bigint' in data_type:
            data_type = "Long"
        elif data_type in ["tinyint", "integer"]:
            data_type = "Integer"
        elif 'datetime' in data_type:
            data_type = "Date"
            date_format = '@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")\n'
        title = arr[3].replace('\',', "").replace('\'', "")
        value = f'/**\n* {title}\n*/\n{date_format}private {data_type} {snake_to_camel(arr[0])};\n'
        lstResult.append(value)
    if '@TableName' in lstResult[0]:
        lstResult.append("}")
    fileUtil.write_lst_txt(lstResult, fileUtil.get_run_path_file('f0_result.txt'), 1)

def sql_to_json(has_title):
    lstSource = fileUtil.read_txt_to_list(fileUtil.get_run_path_file('f0_source.txt'))

    lstResult = ["{"]
    for i in range(len(lstSource)):
        item = lstSource[i]
        if ('CREATE TABLE' in item or "create_time" in item or "update_time" in item
                or "unique" in item or "COMMENT=" in item):
            continue

        # 提取字段
        item = remove_comment_spaces(item)
        arr = re.sub(r'\s+', ' ', item.strip()).split(" ")
        data_type = arr[1].lower()
        value = '""'
        if data_type in ["tinyint", "integer", "bigint"]:
            value = 1
        title = ""
        if has_title:
            title = "//" + arr[3].replace('\'', "")
        if (i == len(lstSource) - 5):
            value = f'"{snake_to_camel(arr[0])}": {value} {title}'
        else:
            value = f'"{snake_to_camel(arr[0])}": {value}, {title}'
        lstResult.append(value)
    lstResult.append("}")
    fileUtil.write_lst_txt(lstResult, fileUtil.get_run_path_file('f0_result.txt'), 1)

def sql_to_jsonschema(sql):
    # 解析字段定义
    fields = []
    in_parentheses = False
    field_lines = []

    # 提取括号内的内容
    for line in sql.split('\n'):
        line = line.strip()
        if '(' in line and not in_parentheses:
            in_parentheses = True
            line = line.split('(', 1)[1]
        if in_parentheses and ')' in line:
            line = line.rsplit(')', 1)[0]
            in_parentheses = False
        if in_parentheses:
            field_lines.append(line)

    # 解析每个字段
    for line in ''.join(field_lines).split(','):
        line = line.strip()
        if not line or line.startswith('constraint') or line.startswith('PRIMARY KEY'):
            continue

        parts = [p.strip() for p in line.split() if p.strip()]
        if not parts:
            continue

        field_name = parts[0]
        fields.append(field_name)

    # 构建 JSON Schema
    schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "x-apifox-orders": fields
    }

    # 添加字段类型映射
    type_mapping = {
        "id": "integer",
        "plat_type": "string",
        "lang_code": "string",
        "field_name": "string",
        "display_name": "string",
        "field_type": "string",
        "operator": "string",
        "enum_values": "array",
        "sort": "integer",
        "status": "integer",
        "create_time": "string",
        "update_time": "string"
    }

    # 添加 NOT NULL 字段到 required
    not_null_fields = [
        "plat_type", "lang_code", "field_name", "display_name",
        "field_type", "sort", "status"
    ]

    # 构建 properties
    for field in fields:
        if field in type_mapping:
            schema["properties"][field] = {"type": type_mapping[field]}

            # 添加特殊格式
            if field in ["create_time", "update_time"]:
                schema["properties"][field]["format"] = "date-time"
            if field == "enum_values":
                schema["properties"][field]["items"] = {"type": "string"}

    # 添加必填字段
    schema["required"] = not_null_fields

    result = json.dumps(schema, indent=2, ensure_ascii=False)
    fileUtil.write_txt(result, fileUtil.get_run_path_file('f0_result.txt'), 0)

if __name__ == "__main__":
    sql_to_entity()
