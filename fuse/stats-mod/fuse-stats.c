#include <linux/module.h>
#include <linux/version.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/kprobes.h>
#include <linux/fuse.h>
#include <linux/proc_fs.h>
#include <asm/uaccess.h>

#include "fuse_i.h"

const char *opcode_names[] = {
    "UNUSED0",
    "LOOKUP",
    "FORGET",
    "GETATTR",
    "SETATTR",
    "READLINK",
    "SYMLINK",
    "UNUSED7",
    "MKNOD",
    "MKDIR",
    "UNLINK",
    "RMDIR",
    "RENAME",
    "LINK",
    "OPEN",
    "READ",
    "WRITE",
    "STATFS",
    "RELEASE",
    "UNUSED19",
    "FSYNC",
    "SETXATTR",
    "GETXATTR",
    "LISTXATTR",
    "REMOVEXATTR",
    "FLUSH",
    "INIT",
    "OPENDIR",
    "READDIR",
    "RELEASEDIR",
    "FSYNCDIR",
    "GETLK",
    "SETLK",
    "SETLKW",
    "ACCESS",
    "CREATE",
    "INTERRUPT",
    "BMAP",
    "DESTROY",
    "IOCTL",
    "POLL",
    "NOTIFY_REPLY",
    "BATCH_FORGET",
    "FALLOCATE",
    "READDIRPLUS",
    "RENAME2"
};

static struct kprobe kp;

#define NUM_OPCODES 46
int opcode_counts[NUM_OPCODES];

int kpb_pre(struct kprobe *p, struct pt_regs *regs)
{
    struct fuse_req *req = (struct fuse_req *)regs->si;

    if (req->in.h.opcode >= NUM_OPCODES) {
        printk(KERN_INFO "!!! opcode out of bounds %d\n", req->in.h.opcode);
        return 0;
    }

    opcode_counts[req->in.h.opcode]++;

    return 0;
}

#define PROCFS_MAX_SIZE 	2048
static char procfs_buffer[PROCFS_MAX_SIZE];

int build_output(void)
{
    int op;
    int pos = 0;
    memset(procfs_buffer, 0, PROCFS_MAX_SIZE);
    for (op=0; op < NUM_OPCODES; op++) {
        if (opcode_counts[op] == 0)
            continue;
        pos += snprintf(procfs_buffer+pos, PROCFS_MAX_SIZE-pos, "%s %d\n", opcode_names[op], opcode_counts[op]);
    }

    return pos;
}

static struct proc_dir_entry *proc_file;

static ssize_t procfs_read(struct file *filp,	/* see include/linux/fs.h   */
			     char *buffer,	/* buffer to fill with data */
			     size_t length,	/* length of the buffer     */
			     loff_t * offset)
{
	static int finished = 0;

	if (finished) {
		printk(KERN_INFO "procfs_read: END\n");
		finished = 0;
		return 0;
	}

    int output_size = build_output();
    if (length < output_size) {
        printk(KERN_INFO "!!! length is too small (%u < %u)\n", length, output_size);
        return 0;
    }
	
	finished = 1;
		
	if (copy_to_user(buffer, procfs_buffer, output_size)) {
		return -EFAULT;
	}

	return output_size;
}

enum cmds {
    CMD_DISABLE = '0',
    CMD_ENABLE = '1',
    CMD_RESET = '2',
};

void reset_stats(void)
{
    int op;
    for (op=0; op < NUM_OPCODES; op++) {
        opcode_counts[op] = 0;
    }
}

static ssize_t
procfs_write(struct file *file, const char *buffer, size_t len, loff_t * off)
{
    unsigned char cmd;

    if (copy_from_user(&cmd, buffer, sizeof(cmd))) {
        return -EFAULT;
    }

    switch(cmd) {
        case CMD_DISABLE:
            disable_kprobe(&kp);
            printk(KERN_INFO "disabling");
            break;
        case CMD_ENABLE:
            reset_stats();
            enable_kprobe(&kp);
            printk(KERN_INFO "enabling");
            break;
        case CMD_RESET:
            reset_stats();
            printk(KERN_INFO "resetting");
            break;
        default:
            return -EINVAL;
    }

    return len;
}

static struct file_operations fuse_stats_ops = {
	.read 	 = procfs_read,
	.write 	 = procfs_write,
    .owner   = THIS_MODULE,
};

int minit(void)
{
    proc_file = proc_create("fuse-stats", 0, NULL, &fuse_stats_ops);
    if (proc_file == NULL) {
        printk(KERN_ALERT "Error creating proc entry\n");
        return -ENOMEM;
    }

    kp.pre_handler = kpb_pre;
    kp.symbol_name = "request_end";
    register_kprobe(&kp);

    return 0;
}

void mexit(void)
{
    unregister_kprobe(&kp);
	remove_proc_entry("fuse-stats", NULL);
}

module_init(minit);
module_exit(mexit);
MODULE_AUTHOR("Isham J. Araia");
MODULE_DESCRIPTION("https://ish-ar.io/");
MODULE_LICENSE("GPL");
