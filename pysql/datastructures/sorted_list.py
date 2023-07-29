import typing as tp


T = tp.TypeVar('T')


class SortedList(list, tp.Generic[T]):

    def __init__(self: tp.List[T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sort()

    def insert_sorted(self, val: T):
        i = 0
        self.insert(i, val)

        # 6 -> [] = [6]
        # 6 -> [1] = [6, 1]
        # i = 0: [6, 1] -> [1, 6]

        # while there's items left and current and second items
        # are not sorted, swap them and move on to the next
        while i < len(self) - 1:
            if self[i] >= self[i + 1]:
                self[i], self[i + 1] = self[i + 1], self[i]
                i += 1
            else:
                return
