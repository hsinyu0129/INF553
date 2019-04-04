import operator
import copy 

class Graph:
    def __init__(self, adjacentDict = None):
        if adjacentDict == None: adjacentDict = {}
        self.originalG = copy.deepcopy(adjacentDict)
        self.adjacentDict = adjacentDict
        self.Community = []
        self.maxModularity = -float('inf')

    def getVertices(self):
        return list(self.originalG.keys())

    def getDegree(self, vertex):
        return len(self.originalG[vertex])

    def getallDegree(self):
        return { v: len(edges) for (v, edges) in self.originalG.items() }
   
    def get2Edges(self):
        return sum([ edges for (v, edges) in self.getallDegree().items()])
    
    def deleteEdge(self, v1, v2): #undirected graph
        self.adjacentDict[v1].remove(v2)
        if not self.adjacentDict[v1]: del self.adjacentDict[v1]
        self.adjacentDict[v2].remove(v1)
        if not self.adjacentDict[v2]: del self.adjacentDict[v2]

    def BFS(self, root):
        # visited dict (vertex:(level, parents))
        visited = dict()
        # queue > order to explore
        queue = []

        # for root
        visited[root] = (0, [])
        queue.append(root)
        if not self.adjacentDict.get(root): return visited

        # BFS
        while queue:
            current = queue.pop(0)
            for adj in self.adjacentDict[current] : 
                if adj not in visited: 
                    queue.append(adj) 
                    visited[adj] = (visited[current][0]+1, [current])
                elif visited[current][0]+1 == visited[adj][0]: visited[adj][1].append(current)
        return visited

    def Betweeness(self):
        between = {}
        # {level: [(child1, [parents]), (child1, [parents])]}
        for vertex in self.getVertices():
            shortest_path = {}
            level = {}
            parentnode = []
            bfstree = self.BFS(vertex)
            for v, l in sorted(bfstree.items(), key = lambda pair: -pair[1][0]):
                level.setdefault(l[0], []).append((v, l[1]))
                parentnode.extend(l[1])

            for l in range(0, len(level)):
                for (child, parents) in level[l]: 
                    if not parents: shortest_path[child] = 1
                    else: shortest_path[child] = sum([shortest_path[parent] for parent in parents])
            # print(shortest_path)
          
            parentnode = set(parentnode)
            nodeweight = {}
            for l, nodes in level.items():
                for (child, parents) in nodes: 
                    #leaf nodes
                    if child not in parentnode: nodeweight[child] = 1
                    else: nodeweight[child] += 1
                    
                    #edge betweenness 
                    allparents = sum([shortest_path[parent] for parent in parents]) 
                    for parent in parents:
                        try: nodeweight[parent] += nodeweight[child]*float(shortest_path[parent])/allparents
                        except: nodeweight[parent] = nodeweight[child]*float(shortest_path[parent])/allparents
                        
                        if parent < child: v1, v2 = parent, child
                        else: v1, v2 = child, parent
                        
                        try: between[(v1, v2)] += nodeweight[child]*float(shortest_path[parent])/allparents
                        except: between[(v1, v2)] = nodeweight[child]*float(shortest_path[parent])/allparents
        return { edge: value/2 for (edge, value) in between.items()}

    def maxBetweeness(self):
        return max(self.Betweeness().items(), key=operator.itemgetter(1))[0]

    def Connectivity(self):
        community = []
        vertex = set(self.getVertices())
        while vertex:
            connectNodes = set(self.BFS(list(vertex)[0]).keys())
            community.append(connectNodes)
            vertex -= connectNodes
        return community

    def Modularity(self):
        modu = 0
        m2 = self.get2Edges()
        for com in self.Connectivity():
            if len(com) == 1: continue
            a, k = 0, 0
            while com:
                i = com.pop()
                a += len(self.originalG[i] & com)
                k += sum([self.getDegree(j) for j in com])*self.getDegree(i)
            modu += a-(k/m2)
        return modu
   
    def detectCommunity(self):
        while self.adjacentDict:
            modu = self.Modularity()
            if modu > self.maxModularity:
                self.maxModularity = modu
                self.Community = self.Connectivity()
            delE = self.maxBetweeness()
            self.deleteEdge(delE[0], delE[1])
        return self.Community


# adjacentDict = {'a': {'b', 'c'}, 'b': {'a', 'd', 'c'}, 'c': {'b', 'a'}, 'd': {'b', 'g', 'e', 'f'}, 'g': {'f', 'd'}, 'f': {'d', 'g', 'e'}, 'e': {'f', 'd'}}
# adjacentDict = {'a': {'b'}, 'b': {'a', 'd', 'g'}, 'd': {'b', 'g', 'h'}, 'g': {'b', 'd', 'h'}, 'h': {'d', 'g'}}
# adjacentDict = {'0': {'1', '2'}, '1': {'0', '2', '3', '4'}, '2': {'0', '1', '3', '4'}, '3': {'1', '2', '4'}, '4': {'1', '2', '3', '5', '6'}, '5': {'4', '6'}, '6': {'4', '5', '7', '8', '9'}, '7': {'6', '8', '9'}, '8': {'6', '7', '9', '10'}, '9': {'6', '7', '8', '10'}, '10': {'8', '9'}}
# g = Graph(adjacentDict)
# print(g.Betweeness())
# community = g.detectCommunity()
# print(community)



