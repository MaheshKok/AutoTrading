class Node:
    def __init__(self, data, left: None, right: None):
        self.data = data
        self.left = left
        self.right = right


def print_zig_zag(root):
    """
    stack_1: right to left
    stack_2: left to right
    """


    stack_1 = []
    stack_2 = [root]

    while len(stack_1) > 0 or len(stack_2) > 0:
        while len(stack_1) > 0:
            popped_ele = stack_1.pop()
            print(popped_ele)
            stack_2.append(popped_ele.left)
            stack_2.append(popped_ele.right)

        while len(stack_2) > 0:
            popped_ele = stack_2.pop()
            print(popped_ele)
            stack_2.append(popped_ele.left)
            stack_2.append(popped_ele.right)
