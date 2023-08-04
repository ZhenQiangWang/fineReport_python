x = []


def test():
    global x
    x = ["c"]
    x.append(2)
    print(x)


def test1():
    print("test1:{}".format(x))


if __name__ == '__main__':
    test()
    test1()
