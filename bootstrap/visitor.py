import ast
#import unparse

class CrazyTransformer(ast.NodeTransformer):
    def __init__(self, calls=[('DAG', 'schedule_interval', '* * * * *')]):
        self.calls = calls
    '''
    def generic_visit(self, node):
        ast.NodeTransformer.generic_visit(self, node)
        return node
    '''
    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            for c in self.calls:
                if node.func.id == c[0]:
                    for keyword in node.keywords:
                        if keyword.arg == c[1]:
                            keyword.value.s = c[2]
        return node
    '''
    def visit_Keyword(self, node):
        return node
    def visit_Num(self, node):
        return node
    def visit_Str(self, node):
        return node
    def visit_Name(self, node):
        return node
    def visit_Alias(self, node):
        node.asname=''
        ast.NodeTransformer.generic_visit(self, node)
        return node
    def visit_Assign(self, node):
        return node
    def visit_Print(self, node):
        return node
    def visit_Expr(self, node):#remove print
        if isinstance(node.value, ast.Call):
            if isinstance(node.value.func,ast.Name):
                if node.value.func.id=='print':
                    return None
        ast.NodeTransformer.generic_visit(self, node)
        return node
        #         else:
        #             print('1')
        #             ast.NodeVisitor.generic_visit(self, node)
        #     else:
        #         print('2')
        ##         ast.NodeVisitor.generic_visit(self, node)
        # else:
        #     print('3')
        #     ast.NodeVisitor.generic_visit(self, node)

    def visit_ClassDef(self,node):
        if hasattr(node,'name'):
            node.name=''
        ast.NodeTransformer.generic_visit(self, node)
        return node
    def visit_FunctionDef(self,node):
        if hasattr(node,'name'):
            # print(node)
            node.name=''
        if hasattr(node,'args'):
            node.args=''
        ast.NodeTransformer.generic_visit(self, node)
        return node
    '''

if __name__ == '__main__':
    import unparse
    from airflow.utils.code_utils import get_python_source
    import pprint
    import d3
    node = get_python_source(d3)
    expr_ast = ast.parse(node)
    #pprint.pprint(ast.dump(expr_ast))
    #node = ast.parse("res= e**(((-0.5*one)*((delta_w*one/delta*one)**2)))")
    #import ast_pretty
    #print(ast.dump(node))
    #pprintAst(node)
    v = CrazyTransformer([('DAG', 'schedule_interval', 'abciiadsflasdfj')])
    expr_new = v.visit(expr_ast)
    ast.fix_missing_locations(expr_new)
    #print(ast.dump(expr_new))
    #unparse.Unparser(expr_new)
    import astunparse
    from astor.code_gen import to_source
    source = astunparse.unparse(expr_new)
    print(ast.unparse(expr_new))
    exit()
    print(source)
    print(to_source(expr_new))
    print(ast.dump(expr_new))
    print(astunparse.dump(expr_new))
    from astmonkey import visitors
    print(visitors.to_source(expr_new))
