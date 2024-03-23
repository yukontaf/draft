#!/Users/gsokolov/anaconda3/bin/python
import json, sys, autopep8, ast

file = sys.argv[1]

def convert_ipynb_to_script(ipynb_file, output_file):
    with open(ipynb_file, 'r') as file:
        notebook = json.load(file)

    script = ""
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code' and len(cell['source']) > 0:
            source = ''.join(cell['source'])
            if cell['source'][0].startswith('%%sql'):
                source = source.replace('%%sql', '').strip()
                print(cell['metadata']['SqlCellData'])
                var_name = cell['metadata']['SqlCellData'].get('variableName$1', 'sql_script')
                script += f"{var_name} = '''{source}'''\n\n"
            else:
                formatted_source = autopep8.fix_code(source, options={'aggressive': 1})
                tree = ast.parse(formatted_source)
                transformer = RemoveUnusedExpressions()
                modified_tree = ast.fix_missing_locations(transformer.visit(tree))
                modified_source = ast.unparse(modified_tree)
                script += modified_source + '\n\n'

    with open(output_file, 'w') as file:
        file.write(script.strip() + '\n')

class RemoveUnusedExpressions(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, (ast.Call, ast.Constant)):
            return None
        return node

convert_ipynb_to_script(file, file.replace('.ipynb', '.py'))
