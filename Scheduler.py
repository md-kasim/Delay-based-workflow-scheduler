import sys
import math
from heapq import heappush, heappop

# Input BEGIN
VM = 3		# No of VMs
CS = 2		# No of Cloud Servers
VM_CS = {1:1, 2:1, 3:2}		# VM to CLoud Map
T = [1,2,3,4,5,6,7,8,9,10]	# Tasks
# Dependency Edges
E = [(1, 2, 18), (1, 3, 12), (1, 4, 9), (1, 5, 11), (1, 6, 14), (2, 8, 19), (2, 9, 16), (3, 7, 23), (4, 8, 27), (4, 9, 23), (5, 9, 13), (6, 8, 15), (7, 10, 17), (8, 10, 11), (9, 10, 13)]
# Estimated Computation Time
ECT = [[14, 13, 11, 13, 12, 13, 7, 5, 18, 21],
        [16, 19, 13, 8, 13, 16, 15, 11, 12, 7],
        [9, 18, 19, 17, 10, 9, 11, 14, 20, 16]]
# Cost of using VM per unit time
VMrate = [1.667, 1.830, 0.430]
# Input END


# Constructing the Graph with the consideration of Dependency Edges and for Topological Sort
class Graph:
    def __init__(self, vertices, edges):
        self.src = 1
        self.V = vertices
        self.E = {}
        self.parent = {}
        self.createAdjacencyList(edges)
        self.printG()


    def createAdjacencyList(self, edges):
        for v in self.V:
            self.parent[v]=[]
        for v in self.V:
            self.E[v] = list(map(lambda x: x[1],list(filter(lambda x: (x[0] == v), edges))))
            for u in self.E[v]:
                self.parent[u].append(v)

    
    def calculate_CPL_utils(self, u, d, visited, path, CPL, DTT, avg_ECT, pre=0, w=0):
        visited[u-1]= True
        path.append(u)
        w += avg_ECT[u-1]
        if pre>0:
            w += DTT[pre-1][u-1]
        if u ==d:
            CPL[-1] = max(CPL[-1], w)
        else:
            for i in self.E[u]:
                if visited[i-1]==False: 
                    self.calculate_CPL_utils(i, d, visited, path, CPL, DTT, avg_ECT, u, w) 
        path.pop()
        visited[u-1]= False
    
    
    def calculate_CPL(self, DTT, avg_ECT):
        visited = [False]*len(self.V)
        path = []
        CPL = [0]
        self.calculate_CPL_utils(self.src, self.V[-1], visited, path, CPL, DTT, avg_ECT)
        return CPL[-1]


    def calculate_CPmin_utils(self, u, d, visited, path, CPmin, DTT, ECT_t, pre=0, w=0, sl=0):
        visited[u-1]= True
        path.append(u)
        w += min(ECT_t[u-1])
        sl += min(ECT_t[u-1])
        if pre>0:
            w += DTT[pre-1][u-1]
        if u == d and CPmin[0] < w:
            CPmin[0], CPmin[1] = w, sl
        else:
            for i in self.E[u]:
                if visited[i-1]==False: 
                    self.calculate_CPmin_utils(i, d, visited, path, CPmin, DTT, ECT_t, u, w, sl) 
        path.pop()
        visited[u-1]= False
    
    
    def calculate_CPmin(self, DTT, ECT_t):
        visited = [False]*len(self.V)
        path = []
        CPmin = [0, 0]
        self.calculate_CPmin_utils(self.src, self.V[-1], visited, path, CPmin, DTT, ECT_t)
        return CPmin


    def topological_sort_util(self, u, visited, stack):
        visited[u-1]=True
        for v in self.E[u]:
            if not visited[v-1]:
                self.topological_sort_util(v, visited, stack)
        stack.append(u)


    def topological_sort(self):
        visited = [False]*len(self.V)
        stack = []
        for u in self.V:
            if not visited[u-1]:
                self.topological_sort_util(u, visited, stack)
        # stack.reverse()
        return stack


    def printG(self):
        print("\nAdjacency List Representation of the Graph")
        for u in self.V:
            print(u, " : ", end="")
            for v in self.E[u]:
                print(v, " -> ", end="")
            print()


# Distance Travel Time
def get_DTT(T, E):
    DTT = [[0]*len(T) for t in T]
    for e in E:
        DTT[e[0]-1][e[1]-1] = e[2]
    return DTT


# Computing ADAP
def compute_ADAP(g, DTT, avg_ECT):
    ADAP = [0]*len(g.V)
    tplgl_order = g.topological_sort()
    CPL = math.ceil(g.calculate_CPL(DTT, avg_ECT)) #Critical Path Length of Workflow
    print("\nCPL : ", CPL)
    for u in tplgl_order:
        min_length = CPL
        for v in g.E[u]:
            if ADAP[v-1] - DTT[u-1][v-1] < min_length:
                min_length = ADAP[v-1]-DTT[u-1][v-1]
        ADAP[u-1] = min_length-avg_ECT[u-1]
    return ADAP


# Generating the schedule for the tasks and Creating VM_Mapping table
def scheduler(g, ADAP, DTT):
	src = [g.src]
	next_src = []
	Avl = [0]*VM
	exec_order = []

	Mkspan = [0]*VM
	AFT = [math.inf]*len(T)
	VMID = [0]*len(T)
	CSID = [0]*len(T)
	VM_mapping = [{"EST":[0]*len(T), "EFT":[0]*len(T)} for vm in range(VM)]

	while src:
	    tasks = []
	    for u in src:
	        if (-1*ADAP[u-1], u) not in tasks: heappush(tasks, (-1*ADAP[u-1], u))
	        if u not in next_src: next_src += g.E[u]
	    while tasks:
	        t = heappop(tasks)[1]
	        exec_order.append(t)
	        parent = g.parent[t]
	        for vm in range(VM):
	            for p in parent:
	                if VM_CS[vm+1]==CSID[p-1]:
	                    VM_mapping[vm]["EST"][t-1]=max(VM_mapping[vm]["EST"][t-1],AFT[p-1])
	                else:
	                    VM_mapping[vm]["EST"][t-1]=max(VM_mapping[vm]["EST"][t-1],AFT[p-1]+DTT[p-1][t-1])
	            VM_mapping[vm]["EST"][t-1] = max(Avl[vm], VM_mapping[vm]["EST"][t-1])
	            VM_mapping[vm]["EFT"][t-1] = VM_mapping[vm]["EST"][t-1]+ECT[vm][t-1]
	            if AFT[t-1]>VM_mapping[vm]["EFT"][t-1]:
	                AFT[t-1]=VM_mapping[vm]["EFT"][t-1]
	                VMID[t-1]=vm+1
	                CSID[t-1]=VM_CS[vm+1]
	        Avl[VMID[t-1]-1] = VM_mapping[VMID[t-1]-1]["EFT"][t-1]
	        Mkspan[VMID[t-1]-1] += VM_mapping[VMID[t-1]-1]["EFT"][t-1] - VM_mapping[VMID[t-1]-1]["EST"][t-1]
	    src = next_src
	    next_src = []
	print("\nOrder   ", end="")
	for vm in range(VM): print("EST  EFT  ", end="")
	print("VMID   AFT  CSID")
	for t in exec_order:
	    print("%5d"%(t),end="")
	    for vm in range(VM):
	        print("%5d%5d"%(VM_mapping[vm]["EST"][t-1], VM_mapping[vm]["EFT"][t-1]), end="")
	    print("%4d%6d%4d"%(VMID[t-1], AFT[t-1], CSID[t-1]))
	return Mkspan, AFT, VMID, CSID, VM_mapping


def main():
	DTT = get_DTT(T, E)
	print("\nData Transfer Time between nodes")
	for dtt in DTT:
	    print(dtt)

	#avg_ECT
	avg_ECT = list(map(lambda x:sum(x)/len(x), zip(*ECT)))
	print("\nAverage Estimated Computation Time : \n",avg_ECT)

	g = Graph(T, E)

	#ADAP
	ADAP = compute_ADAP(g, DTT, avg_ECT)
	print("\nADAP : ",ADAP)

	Mkspan, AFT, VMID, CSID, VM_mapping = scheduler(g, ADAP, DTT)

	# 1. Makespan 

	# 2. Energy Consumption 

	# 3. Load balancing 

	# 4. VM Utilization 

	# 5. Speedup

	# 6. SLR

	# SL
	SL = g.calculate_CPmin(DTT, list(zip(*ECT)))[1]

	# Make Span
	MKS = AFT[-1]

	# Cost Utilization
	Cost_VM = [0]*VM
	for vm in range(VM):
	    Cost_VM[vm] = Mkspan[vm]*VMrate[vm]
	total_cost = sum(Cost_VM)

	# Execution Time -> Enery Consumption
	Ebusy, Eidle = .7, .2
	energy_utilization = sum(map(lambda x:x*Ebusy + (SL-x)*Eidle, Mkspan))

	# Load Balancing
	AT = [0]*VM
	for vm in range(VM):
	    AT[vm] = sum(list(map(lambda x, y: x if y==vm+1 else 0, ECT[vm], VMID)))
	avg_AT = sum(AT)/len(AT)
	SD = (sum(list(map(lambda x:(x-avg_AT)**2, AT)))/VM)**.5

	#VM Utilization
	utilization = list(map(lambda x: x/MKS, AT))
	avg_U = sum(utilization)/VM

	#Speed up
	sum_ECT = []
	for vm in range(VM):
	    sum_ECT.append(sum(ECT[vm]))
	speed_up = min(sum_ECT)/MKS

	# SLR
	SLR = MKS/SL

	print("\nSchedule Length : ",SL)
	print("\nMkspan : ", Mkspan)
	print("\nMKS : ",MKS)
	print("\nTotal Cost Utilization : ",total_cost)
	print("\nTotal Energy Utilization : ",energy_utilization)
	print("\nStandard Deviation of Active time : ",SD)
	print("\nAverage VM Utilization : ",avg_U)
	print("\nSpeed up : ",speed_up)
	print("\nSchedule Length Ratio : ",SLR)


if __name__ == '__main__':

	# Opening result.txt file for storing result
	outputFile = open("result.txt", "w")
	orig_stdout = sys.stdout
	sys.stdout = outputFile
	
	# running the program
	main()
	
	# closing the file
	sys.stdout = orig_stdout
	outputFile.close()
