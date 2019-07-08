from gevent import  pool ,monkey
monkey.patch_all()
import gevent

a  = 0



def test():
    global  a
    a +=1
    print(a)
    time.sleep(3)



p = pool.Pool(10)
import  time

for i in range(100):
    p = gevent.spawn(test())
    p.start()


print(a)