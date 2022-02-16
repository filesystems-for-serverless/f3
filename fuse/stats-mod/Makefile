obj-m +=fuse-stats.o
KDIR= /lib/modules/$(shell uname -r)/build

all:
	$(MAKE) -C $(KDIR) SUBDIRS=$(PWD) modules

in:
	sudo insmod fuse-stats.ko

rm:
	sudo rmmod fuse-stats

log:
	dmesg | tail -n10

clean:
	rm -rf *.o *.ko *.mod.* .c* .t*

install:
	bash install.sh
