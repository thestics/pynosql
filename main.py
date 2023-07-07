from pysql.datastructures.rb_set import IntSet
from pysql.datastructures.rbtree import RedBlackTree



if __name__ == '__main__':
    s = IntSet([(10, 'a'), (2, 'b'), (3, 'c'), (5, 'foo'), (11, 'bar')])
    s._tree.print_tree()
    print(3 in s)
    print(10 in s)
    d = s.dump()
    print(d)
    d1 = IntSet.load(d)
    d1._tree.print_tree()

