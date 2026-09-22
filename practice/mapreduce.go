func InitMapReduce(nmap int, nreduce int,
	file string, master string) *MapReduce {

	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = file
	mr.MasterAddress = master
	mr.alive = true
	mr.registerChannel = make(chan string)
	mr.DoneChannel = make(chan bool)

	mr.Workers = make(map[string]*WorkerInfo)

	return mr
}
