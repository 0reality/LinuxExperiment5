/*
 * Sample disk driver, from the beginning.
 * 示例磁盘驱动程序,从头开始实现
 */

#include <linux/version.h> 	/* LINUX_VERSION_CODE  */
#include <linux/blk-mq.h>	/* 块设备多队列支持 */	
/* https://olegkutkov.me/2020/02/10/linux-block-device-driver/
   https://prog.world/linux-kernel-5-0-we-write-simple-block-device-under-blk-mq/           
   blk-mq and kernels >= 5.0
*/


#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/sched.h>
#include <linux/kernel.h>	/* printk() */
#include <linux/slab.h>		/* kmalloc() */
#include <linux/fs.h>		/* everything... */
#include <linux/errno.h>	/* error codes */
#include <linux/timer.h>
#include <linux/types.h>	/* size_t */
#include <linux/fcntl.h>	/* O_ACCMODE */
#include <linux/hdreg.h>	/* HDIO_GETGEO */
#include <linux/kdev_t.h>
#include <linux/vmalloc.h>
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>	/* invalidate_bdev */
#include <linux/bio.h>

MODULE_LICENSE("Dual BSD/GPL");

/* 模块参数: 主设备号,0表示动态分配 */
static int sbull_major = 0;
module_param(sbull_major, int, 0);
static int hardsect_size = 512;	/* 硬件扇区大小 */
module_param(hardsect_size, int, 0);
static int nsectors = 1024;	/* How big the drive is 设备扇区数量 */
module_param(nsectors, int, 0);
static int ndevices = 4;	/* 设备数量 */
module_param(ndevices, int, 0);

/*
 * The different "request modes" we can use.
 * 我们可以使用的不同"请求模式"
 */
enum {
	RM_SIMPLE  = 0,	/* The extra-simple request function 简单请求处理 */
	RM_FULL    = 1,	/* The full-blown version 完整请求处理 */
	RM_NOQUEUE = 2,	/* Use make_request 直接使用make_request */
};
static int request_mode = RM_SIMPLE;	/* 默认使用简单模式 */
module_param(request_mode, int, 0);

/*
 * Minor number and partition management.
 * 次设备号和分区管理
 */
#define SBULL_MINORS	16	/* 每个设备支持的分区数 */
#define MINOR_SHIFT	4	/* 次设备号移位数(2^4=16) */
#define DEVNUM(kdevnum)	(MINOR(kdev_t_to_nr(kdevnum)) >> MINOR_SHIFT)	/* 从次设备号获取设备号 */

/*
 * We can tweak our hardware sector size, but the kernel talks to us
 * in terms of small sectors, always.
 * 我们可以调整硬件扇区大小,但内核总是以小扇区为单位与我们通信
 */
#define KERNEL_SECTOR_SIZE	512	/* 内核扇区大小 */

/*
 * After this much idle time, the driver will simulate a media change.
 * 设备空闲这么长时间后,驱动程序将模拟介质更换
 */
#define INVALIDATE_DELAY	30*HZ	/* 30秒后模拟介质更换 */

/*
 * The internal representation of our device.
 * 设备的内部表示结构
 */
struct sbull_dev {
        int size;                       /* Device size in sectors 设备大小(扇区) */
        u8 *data;                       /* The data array 存储数据的数组 */
        short users;                    /* How many users 用户计数 */
        short media_change;             /* Flag a media change? 介质更换标志 */
        spinlock_t lock;                /* For mutual exclusion 自旋锁,用于互斥 */
	struct blk_mq_tag_set tag_set;	/* tag_set added 块设备多队列标签集 */
        struct request_queue *queue;    /* The device request queue 设备请求队列 */
        struct gendisk *gd;             /* 通用磁盘结构，用于注册设备 */
        struct timer_list timer;        /* For simulated media changes 模拟介质更换的定时器 */
};

static struct sbull_dev *Devices = NULL;	/* 设备数组指针 */

/**
* See https://github.com/openzfs/zfs/pull/10187/
* 根据内核版本差异实现兼容的请求队列分配函数
*/
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 9, 0))
static inline struct request_queue *
blk_generic_alloc_queue(make_request_fn make_request, int node_id)	/* 5.9以下版本 */
#else
static inline struct request_queue *
blk_generic_alloc_queue(int node_id)	/* 5.9及以上版本 */
#endif
{
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 7, 0))
	struct request_queue *q = blk_alloc_queue(GFP_KERNEL);
	if (q != NULL)
		blk_queue_make_request(q, make_request);

	return (q);
#elif (LINUX_VERSION_CODE < KERNEL_VERSION(5, 9, 0))
	return (blk_alloc_queue(make_request, node_id));
#else
	return (blk_alloc_queue(node_id));
#endif
}

/*
 * Handle an I/O request.
 * 处理I/O请求,执行实际的数据传输
 */
static void sbull_transfer(struct sbull_dev *dev, unsigned long sector,
		unsigned long nsect, char *buffer, int write)
{
	unsigned long offset = sector * hardsect_size;	/* 计算扇区偏移量 */
	unsigned long nbytes = nsect * hardsect_size;	/* 计算传输字节数 */

	/* 检查是否超出设备范围 */
	if ((offset + nbytes) > dev->size) {
		printk(KERN_NOTICE "sbull: Beyond-end write (%ld %ld)\n", offset, nbytes);
		return;
	}

	/* 根据读写标志执行数据传输 */
	if (write) {
		/* 写操作: 从buffer复制到设备数据区 */
		memcpy(dev->data + offset, buffer, nbytes);
	} else {
		/* 读操作: 从设备数据区复制到buffer */
		memcpy(buffer, dev->data + offset, nbytes);
	}
}

/*
 * The simple form of the request function.
 * 简单请求函数,处理blk-mq队列中的请求
 */
//static void sbull_request(struct request_queue *q)
static blk_status_t sbull_request(struct blk_mq_hw_ctx *hctx, const struct blk_mq_queue_data* bd)   /* For blk-mq */
{
	struct request *req = bd->rq;	/* 获取请求结构 */
	struct sbull_dev *dev = req->rq_disk->private_data;	/* 获取设备私有数据 */
        struct bio_vec bvec;		/* bio向量,表示一个内存段 */
        struct req_iterator iter;	/* 请求迭代器 */
        sector_t pos_sector = blk_rq_pos(req);	/* 请求的起始扇区 */
	void	*buffer;		/* 数据缓冲区指针 */
	blk_status_t  ret;		/* 返回状态 */

	blk_mq_start_request (req);	/* 开始处理请求 */

	if (blk_rq_is_passthrough(req)) {	/* 跳过非文件系统请求 */
		printk (KERN_NOTICE "Skip non-fs request\n");
                ret = BLK_STS_IOERR;  //-EIO
			goto done;
	}
	/* 遍历请求的每个段 */
	rq_for_each_segment(bvec, req, iter)
	{
		size_t num_sector = blk_rq_cur_sectors(req);	/* 当前请求的扇区数 */
		printk (KERN_NOTICE "Req dev %u dir %d sec %lld, nr %ld\n",
                        (unsigned)(dev - Devices), rq_data_dir(req),
                        pos_sector, num_sector);
		buffer = page_address(bvec.bv_page) + bvec.bv_offset;	/* 获取数据缓冲区地址 */
		sbull_transfer(dev, pos_sector, num_sector,	/* 执行数据传输 */
				buffer, rq_data_dir(req) == WRITE);
		pos_sector += num_sector;	/* 更新扇区位置 */
	}
	ret = BLK_STS_OK;	/* 返回成功状态 */
done:
	blk_mq_end_request (req, ret);	/* 结束请求处理 */
	return ret;
}


/*
 * Transfer a single BIO.
 * 传输单个BIO请求
 */
static int sbull_xfer_bio(struct sbull_dev *dev, struct bio *bio)
{
	struct bio_vec bvec;		/* bio向量 */
	struct bvec_iter iter;		/* bio迭代器 */
	sector_t sector = bio->bi_iter.bi_sector;	/* 起始扇区 */

	/* Do each segment independently. 独立处理每个段 */
	bio_for_each_segment(bvec, bio, iter) {
		//char *buffer = __bio_kmap_atomic(bio, i, KM_USER0);
		char *buffer = kmap_atomic(bvec.bv_page) + bvec.bv_offset;	/* 映射页面获取缓冲区 */
		//sbull_transfer(dev, sector, bio_cur_bytes(bio) >> 9,
		sbull_transfer(dev, sector, (bio_cur_bytes(bio) / KERNEL_SECTOR_SIZE),
				buffer, bio_data_dir(bio) == WRITE);	/* 传输数据 */
		//sector += bio_cur_bytes(bio) >> 9;
		sector += (bio_cur_bytes(bio) / KERNEL_SECTOR_SIZE);	/* 更新扇区位置 */
		//__bio_kunmap_atomic(buffer, KM_USER0);
		kunmap_atomic(buffer);	/* 解除映射 */
	}
	return 0; /* Always "succeed" 总是返回成功 */
}

/*
 * Transfer a full request.
 * 传输完整的请求(可能包含多个BIO)
 */
static int sbull_xfer_request(struct sbull_dev *dev, struct request *req)
{
	struct bio *bio;	/* bio结构指针 */
	int nsect = 0;		/* 传输的扇区总数 */

	/* 遍历请求中的每个BIO */
	__rq_for_each_bio(bio, req) {
		sbull_xfer_bio(dev, bio);	/* 传输单个BIO */
		//nsect += bio->bi_size/KERNEL_SECTOR_SIZE;
		nsect += bio->bi_iter.bi_size/KERNEL_SECTOR_SIZE;	/* 累加扇区数 */
	}
	return nsect;	/* 返回传输的总扇区数 */
}



/*
 * Smarter request function that "handles clustering".
 * 更智能的请求函数,可以处理聚簇请求
 */
//static void sbull_full_request(struct request_queue *q)
static blk_status_t sbull_full_request(struct blk_mq_hw_ctx * hctx, const struct blk_mq_queue_data * bd)
{
	struct request *req = bd->rq;	/* 获取请求结构 */
	int sectors_xferred;		/* 已传输的扇区数 */
	//struct sbull_dev *dev = q->queuedata;
	struct sbull_dev *dev = req->q->queuedata;	/* 获取设备数据 */
	blk_status_t  ret;		/* 返回状态 */

	blk_mq_start_request (req);	/* 开始处理请求 */
	//while ((req = blk_fetch_request(q)) != NULL) {
		//if (req->cmd_type != REQ_TYPE_FS) {
		if (blk_rq_is_passthrough(req)) {	/* 跳过非文件系统请求 */
			printk (KERN_NOTICE "Skip non-fs request\n");
			//__blk_end_request(req, -EIO, blk_rq_cur_bytes(req));
			ret = BLK_STS_IOERR; //-EIO;
			//continue;
			goto done;
		}
		sectors_xferred = sbull_xfer_request(dev, req);	/* 传输请求 */
		ret = BLK_STS_OK;	/* 返回成功状态 */
	done:
		//__blk_end_request(req, 0, sectors_xferred);
		blk_mq_end_request (req, ret);	/* 结束请求处理 */
	//}
	return ret;
}



/*
 * The direct make request version.
 * 直接使用make_request的版本
 */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 9, 0))
//static void sbull_make_request(struct request_queue *q, struct bio *bio)
static blk_qc_t sbull_make_request(struct request_queue *q, struct bio *bio)
#else
static blk_qc_t sbull_make_request(struct bio *bio)
#endif
{
	//struct sbull_dev *dev = q->queuedata;
	struct sbull_dev *dev = bio->bi_disk->private_data;	/* 获取设备数据 */
	int status;

	status = sbull_xfer_bio(dev, bio);	/* 传输BIO */
	bio->bi_status = status;		/* 设置BIO状态 */
	bio_endio(bio);			/* 结束BIO处理 */
	return BLK_QC_T_NONE;		/* 返回队列完成标记 */
}


/*
 * Open and close.
 * 设备打开和关闭函数
 */

static int sbull_open(struct block_device *bdev, fmode_t mode)
{
	struct sbull_dev *dev = bdev->bd_disk->private_data;	/* 获取设备数据 */

	del_timer_sync(&dev->timer);	/* 删除定时器 */
	//filp->private_data = dev;
	spin_lock(&dev->lock);		/* 加锁 */
	if (! dev->users) 
	{
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 10, 0))
		check_disk_change(bdev);	/* 检查磁盘是否更换 */
#else
                /* For newer kernels (as of 5.10), bdev_check_media_change()
                 * is used, in favor of check_disk_change(),
                 * with the modification that invalidation
                 * is no longer forced. 5.10及以上使用新的接口 */

                if(bdev_check_media_change(bdev))	/* 检查介质是否更换 */
                {
                        struct gendisk *gd = bdev->bd_disk;
                        const struct block_device_operations *bdo = gd->fops;
                        if (bdo && bdo->revalidate_disk)
                                bdo->revalidate_disk(gd);	/* 重新验证磁盘 */
                }
#endif
	}
	dev->users++;	/* 增加用户计数 */
	spin_unlock(&dev->lock);	/* 解锁 */
	return 0;
}

static void sbull_release(struct gendisk *disk, fmode_t mode)
{
	struct sbull_dev *dev = disk->private_data;	/* 获取设备数据 */

	spin_lock(&dev->lock);		/* 加锁 */
	dev->users--;			/* 减少用户计数 */
	if (dev->users == 0) {		/* 如果没有用户了,重启定时器 */
		dev->timer.expires = jiffies + INVALIDATE_DELAY;
		add_timer(&dev->timer);
	}
	spin_unlock(&dev->lock);	/* 解锁 */
}

/*
 * Look for a (simulated) media change.
 * 检查是否有(模拟的)介质更换
 */
int sbull_media_changed(struct gendisk *gd)
{
	struct sbull_dev *dev = gd->private_data;	/* 获取设备数据 */

	return dev->media_change;	/* 返回介质更换标志 */
}

/*
 * Revalidate.  WE DO NOT TAKE THE LOCK HERE, for fear of deadlocking
 * with open.  That needs to be reevaluated.
 * 重新验证磁盘。这里不加锁,避免与open死锁
 */
int sbull_revalidate(struct gendisk *gd)
{
	struct sbull_dev *dev = gd->private_data;	/* 获取设备数据 */

	if (dev->media_change) {	/* 如果介质已更换 */
		dev->media_change = 0;	/* 清除标志 */
		memset (dev->data, 0, dev->size);	/* 清空设备数据 */
	}
	return 0;
}

/*
 * The "invalidate" function runs out of the device timer; it sets
 * a flag to simulate the removal of the media.
 * 失效函数由设备定时器调用,设置标志以模拟介质移除
 */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4, 15, 0)) && !defined(timer_setup)
void sbull_invalidate(unsigned long ldev)	/* 4.15以下版本的定时器回调 */
{
        struct sbull_dev *dev = (struct sbull_dev *) ldev;
#else
void sbull_invalidate(struct timer_list * ldev)	/* 4.15及以上版本的定时器回调 */
{
        struct sbull_dev *dev = from_timer(dev, ldev, timer);
#endif

	spin_lock(&dev->lock);	/* 加锁 */
	if (dev->users || !dev->data)	/* 检查是否有用户或数据 */
		printk (KERN_WARNING "sbull: timer sanity check failed\n");
	else
		dev->media_change = 1;	/* 设置介质更换标志 */
	spin_unlock(&dev->lock);	/* 解锁 */
}

/*
 * The ioctl() implementation
 * ioctl系统调用实现
 */

int sbull_ioctl (struct block_device *bdev, fmode_t mode,
                 unsigned int cmd, unsigned long arg)
{
	long size;
	struct hd_geometry geo;		/* 硬盘几何参数 */
	struct sbull_dev *dev = bdev->bd_disk->private_data;	/* 获取设备数据 */
	switch(cmd) {
	    case HDIO_GETGEO:	/* 获取硬盘几何参数 */
        	/*
		 * Get geometry: since we are a virtual device, we have to make
		 * up something plausible.  So we claim 16 sectors, four heads,
		 * and calculate the corresponding number of cylinders.  We set the
		 * start of data at sector four.
		 * 获取几何参数:因为我们是虚拟设备,需要编造一个合理的值
		 */
		size = dev->size*(hardsect_size/KERNEL_SECTOR_SIZE);
		geo.cylinders = (size & ~0x3f) >> 6;	/* 柱面数 */
		geo.heads = 4;		/* 磁头数 */
		geo.sectors = 16;	/* 每磁道扇区数 */
		geo.start = 4;		/* 数据起始扇区 */
		if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
			return -EFAULT;
		return 0;
	}

	return -ENOTTY; /* unknown command 未知命令 */
}



/*
 * The device operations structure.
 * 块设备操作结构
 */
static struct block_device_operations sbull_ops = {
	.owner           = THIS_MODULE,	/* 模块所有者 */
	.open 	         = sbull_open,		/* 打开函数 */
	.release 	 = sbull_release,	/* 释放函数 */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 9, 0))
	.media_changed   = sbull_media_changed,  // DEPRECATED in v5.9 介质变化检测
#else
	.submit_bio      = sbull_make_request,	/* 提交BIO */
#endif
	.revalidate_disk = sbull_revalidate,	/* 重新验证磁盘 */
	.ioctl	         = sbull_ioctl		/* ioctl控制 */
};

static struct blk_mq_ops mq_ops_simple = {
    .queue_rq = sbull_request,	/* 简单模式请求处理函数 */
};

static struct blk_mq_ops mq_ops_full = {
    .queue_rq = sbull_full_request,	/* 完整模式请求处理函数 */
};


/*
 * Set up our internal device.
 * 设置内部设备
 */
static void setup_device(struct sbull_dev *dev, int which)
{
	/*
	 * Get some memory.
	 * 分配内存并初始化设备
	 */
	memset (dev, 0, sizeof (struct sbull_dev));
	dev->size = nsectors*hardsect_size;	/* 计算设备大小 */
	dev->data = vmalloc(dev->size);		/* 分配虚拟内存 */
	if (dev->data == NULL) {
		printk (KERN_NOTICE "vmalloc failure.\n");
		return;
	}
	spin_lock_init(&dev->lock);	/* 初始化自旋锁 */

	/*
	 * The timer which "invalidates" the device.
	 * 初始化定时器,用于模拟介质更换
	 */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4, 15, 0)) && !defined(timer_setup)
	init_timer(&dev->timer);
	dev->timer.data = (unsigned long) dev;
	dev->timer.function = sbull_invalidate;
#else
        timer_setup(&dev->timer, sbull_invalidate, 0);
#endif



	/*
	 * The I/O queue, depending on whether we are using our own
	 * make_request function or not.
	 * 根据请求模式设置I/O队列
	 */
	switch (request_mode) {
	    case RM_NOQUEUE:	/* 无队列模式 */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(5, 9, 0))
		dev->queue =  blk_generic_alloc_queue(sbull_make_request, NUMA_NO_NODE);
#else
		dev->queue =  blk_generic_alloc_queue(NUMA_NO_NODE);
#endif
		if (dev->queue == NULL)
			goto out_vfree;
		break;

	    case RM_FULL:	/* 完整模式 */
		//dev->queue = blk_init_queue(sbull_full_request, &dev->lock);
		dev->queue = blk_mq_init_sq_queue(&dev->tag_set, &mq_ops_full, 128, BLK_MQ_F_SHOULD_MERGE);
		if (dev->queue == NULL)
			goto out_vfree;
		break;

	    default:
		printk(KERN_NOTICE "Bad request mode %d, using simple\n", request_mode);
        	/* fall into.. */

	    case RM_SIMPLE:	/* 简单模式 */
		//dev->queue = blk_init_queue(sbull_request, &dev->lock);
		dev->queue = blk_mq_init_sq_queue(&dev->tag_set, &mq_ops_simple, 128, BLK_MQ_F_SHOULD_MERGE);
		if (dev->queue == NULL)
			goto out_vfree;
		break;
	}
	blk_queue_logical_block_size(dev->queue, hardsect_size);	/* 设置逻辑块大小 */
	dev->queue->queuedata = dev;	/* 设置队列私有数据 */
	/*
	 * And the gendisk structure.
	 * 分配并初始化gendisk结构
	 */
	dev->gd = alloc_disk(SBULL_MINORS);
	if (! dev->gd) {
		printk (KERN_NOTICE "alloc_disk failure\n");
		goto out_vfree;
	}
	dev->gd->major = sbull_major;		/* 主设备号 */
	dev->gd->first_minor = which*SBULL_MINORS;	/* 起始次设备号 */
	dev->gd->fops = &sbull_ops;		/* 操作函数 */
	dev->gd->queue = dev->queue;		/* 请求队列 */
	dev->gd->private_data = dev;		/* 私有数据 */
	snprintf (dev->gd->disk_name, 32, "sbull%c", which + 'a');	/* 设备名称 */
	set_capacity(dev->gd, nsectors*(hardsect_size/KERNEL_SECTOR_SIZE));	/* 设置容量 */
	add_disk(dev->gd);	/* 添加到系统 */
	return;

  out_vfree:
	if (dev->data)
		vfree(dev->data);	/* 释放内存 */
}



static int __init sbull_init(void)
{
	int i;
	/*
	 * Get registered.
	 * 注册块设备驱动
	 */
	sbull_major = register_blkdev(sbull_major, "sbull");	/* 注册主设备号 */
	if (sbull_major <= 0) {
		printk(KERN_WARNING "sbull: unable to get major number\n");
		return -EBUSY;
	}
	/*
	 * Allocate the device array, and initialize each one.
	 * 分配设备数组并初始化每个设备
	 */
	Devices = kmalloc(ndevices*sizeof (struct sbull_dev), GFP_KERNEL);	/* 分配设备数组 */
	if (Devices == NULL)
		goto out_unregister;
	for (i = 0; i < ndevices; i++)
		setup_device(Devices + i, i);	/* 初始化每个设备 */

	return 0;

  out_unregister:
	unregister_blkdev(sbull_major, "sbd");	/* 注销块设备 */
	return -ENOMEM;
}

static void sbull_exit(void)
{
	int i;

	/* 清理每个设备 */
	for (i = 0; i < ndevices; i++) {
		struct sbull_dev *dev = Devices + i;

		del_timer_sync(&dev->timer);	/* 删除定时器 */
		if (dev->gd) {
			del_gendisk(dev->gd);	/* 删除gendisk */
			put_disk(dev->gd);	/* 释放gendisk */
		}
		if (dev->queue) {
			if (request_mode == RM_NOQUEUE)
				//kobject_put (&dev->queue->kobj);
				blk_put_queue(dev->queue);	/* 释放队列 */
			else
				blk_cleanup_queue(dev->queue);	/* 清理队列 */
		}
		if (dev->data)
			vfree(dev->data);	/* 释放数据内存 */
	}
	unregister_blkdev(sbull_major, "sbull");	/* 注销块设备 */
	kfree(Devices);	/* 释放设备数组 */
}

module_init(sbull_init);	/* 模块初始化入口 */
module_exit(sbull_exit);	/* 模块退出入口 */
