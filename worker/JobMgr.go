package master

import (
	"crontab/common"
	"encoding/json"
	"github.com/astaxie/beego/logs"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"time"
)

//
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_Conf.EtcdEndpoints,
		DialTimeout: time.Duration(G_Conf.EtcdDialTimeout) * time.Millisecond,
	}
	// create client
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// get kv and lease 获取kv和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 将任务保存到/cron/jobs/任务名
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	// etcd保存key
	jobKey = "/cron/jobs/" + job.Name
	// 任务信息序列化成json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果是更新，返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			logs.Warn("old job value unmarshal failed, err: %v", err)
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// etcd中保存的key
	jobKey = "/cron/jobs/" + name

	// 从etcd中删除它
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 列出所有任务
func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		jobObj  *common.Job
	)
	// 任务保存目录
	dirKey = common.JOB_SAVE_DIR

	// 获取目录下所有key
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 遍历所有任务，进行反序列化
	jobList = make([]*common.Job, 0)
	for _, kvPair = range getResp.Kvs {
		jobObj = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, &jobObj); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, jobObj)
	}
	return
}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名
	var (
		killKey   string
		grantResp *clientv3.LeaseGrantResponse
		leaseId   clientv3.LeaseID
	)
	// 通知worker杀死相应的进程
	killKey = common.JOB_KILL_DIR + name
	// 让worker 监听一次put操作， 创建一个租约，让其稍后自动过期即可
	if grantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}
	leaseId = grantResp.ID

	if _, err = jobMgr.kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}
