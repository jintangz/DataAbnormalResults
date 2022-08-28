from typing import AnyStr, Optional
class Column:
    """
    对应于hive中的列属性
    """
    __slots__ =  ['name', 'type', 'comment']
    def __init__(self, c_name: Optional[AnyStr], c_type: Optional[AnyStr], c_comment: Optional[AnyStr]):
        self.name = c_name
        self.type = c_type
        self.comment = c_comment

    def __str__(self):
        return f'`{self.name}` {self.type} {self.comment}'

if __name__ == '__main__':
    column = Column('lele', 'string', '')
    print(column.__str__() + ',')