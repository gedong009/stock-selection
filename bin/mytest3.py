#encoding: utf-8
import threading
import test


def work():
    # obj = test.test_class()
    # print(obj.print())
    obj = test.test_class().print()
    # test.print()

if __name__ == '__main__':
    print("dsf")
    # 建立一个新数组
    threads = []
    # n = 10
    n = 10
    counter = 1
    while counter <= n:
        name = "Thread-" + str(counter)
        threads.append(threading.Thread(target=work))
        counter += 1
    # threads.append(thing5)
# 写个for让两件事情都进行
    for thing in threads:
        # setDaemon为主线程启动了线程matter1和matter2
        # 启动也就是相当于执行了这个for循环
        thing.setDaemon(True)
        thing.start()

    for thing in threads:
        thing.join()
    global name
    print(name)