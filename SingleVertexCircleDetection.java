package com.geaflow.demo;

import com.alibaba.geaflow.api.graph.Edge;
import com.alibaba.geaflow.api.graph.GraphEnvironment;
import com.alibaba.geaflow.api.graph.Vertex;
import com.alibaba.geaflow.api.pregel.ComputeFunction;
import com.alibaba.geaflow.api.pregel.Message;
import com.alibaba.geaflow.api.pregel.MessagingFunction;
import com.alibaba.geaflow.api.pregel.PregelExecutionEnvironment;
import com.alibaba.geaflow.api.pregel.VertexCentricConfiguration;
import com.alibaba.geaflow.api.window.time.TimeWindow;
import com.alibaba.geaflow.common.type.KeyValuePair;
import com.alibaba.geaflow.common.type.Tuple2;
import com.alibaba.geaflow.common.type.Tuple3;
import com.alibaba.geaflow.datastream.window.TimeWindows;
import com.alibaba.geaflow.graph.utils.EdgeUtils;
import com.alibaba.geaflow.graph.utils.VertexUtils;
import com.alibaba.geaflow.runtime.config.RuntimeOption;

import java.util.*;

public class SingleVertexCircleDetection {

    static class CircleState {
        private String source;       
        private List<String> path;      
        private boolean isVisited;     
        
        public CircleState(String source) {
            this.source = source;
            this.path = new ArrayList<>();
            this.isVisited = false;
        }
        
        public CircleState copy() {
            CircleState newState = new CircleState(source);
            newState.path = new ArrayList<>(path);
            newState.isVisited = isVisited;
            return newState;
        }
        
       
        public String getSource() { return source; }
        public List<String> getPath() { return path; }
        public void setPath(List<String> path) { this.path = path; }
        public void addToPath(String vertexId) { this.path.add(vertexId); }
        public boolean isVisited() { return isVisited; }
        public void setVisited(boolean visited) { isVisited = visited; }
    }
    
  
    static class CircleComputeFunction extends ComputeFunction<String, CircleState, String, Tuple3<String, List<String>, Boolean>> {
        
        private String source;
        private int minCircleLength;
        private int maxCircleLength;
        
        public CircleComputeFunction(String source, int minCircleLength, int maxCircleLength) {
            this.source = source;
            this.minCircleLength = minCircleLength;
            this.maxCircleLength = maxCircleLength;
        }
        
        @Override
        public void compute(Vertex<String, CircleState> vertex,
                           Iterable<Message<String, Tuple3<String, List<String>, Boolean>>> messages) {
            CircleState state = vertex.getValue();
            
          
            if (vertex.getId().equals(source) && state.getPath().isEmpty()) {
                state.addToPath(source);
                state.setVisited(true);
                
         
                for (Edge<String, String> edge : vertex.getOutEdges()) {
                    sendMessage(edge.getTargetId(), 
                               Tuple3.of(source, new ArrayList<>(state.getPath()), false));
                }
                return;
            }
            
        
            for (Message<String, Tuple3<String, List<String>, Boolean>> message : messages) {
                Tuple3<String, List<String>, Boolean> msgValue = message.getValue();
                String msgSource = msgValue.f0;
                List<String> msgPath = msgValue.f1;
                boolean isCircle = msgValue.f2;
                
               
                if (isCircle) {
                    collectCircle(msgSource, msgPath);
                    continue;
                }
                
                if (vertex.getId().equals(msgSource) && msgPath.size() >= minCircleLength) {
                    List<String> circlePath = new ArrayList<>(msgPath);
                    circlePath.add(vertex.getId());
                    collectCircle(msgSource, circlePath);
                    continue;
                }
                
                if (msgPath.contains(vertex.getId())) {
                    continue;
                }
                
          
                List<String> newPath = new ArrayList<>(msgPath);
                newPath.add(vertex.getId());
                
                if (newPath.size() < maxCircleLength) {
                    for (Edge<String, String> edge : vertex.getOutEdges()) {
                        sendMessage(edge.getTargetId(), 
                                   Tuple3.of(msgSource, newPath, false));
                    }
                }
            }
            
           
            state.setVisited(true);
        }
        
        
        private void collectCircle(String source, List<String> path) {
            List<String> sortedPath = new ArrayList<>(path);
            Collections.sort(sortedPath);
            String circleKey = String.join(",", sortedPath);
            
            System.out.println("找到环路（源节点：" + source + "）：" + String.join("->", path));
        }
    }
    
    
    static class CircleMessagingFunction extends MessagingFunction<String, CircleState, String, Tuple3<String, List<String>, Boolean>> {
        @Override
        public void sendMessages(Vertex<String, CircleState> vertex) {
          
        }
    }
    
 
    public static void main(String[] args) {
        try {
          
            GraphEnvironment graphEnv = GraphEnvironment.get();
            PregelExecutionEnvironment env = graphEnv.getPregelEnvironment();
            
         
            env.getConfig().set(RuntimeOption.TASK_PARALLELISM, 4);
            env.getConfig().set(VertexCentricConfiguration.VERTEX_CENTRIC_MAX_SUPERSTEP, 100);
            
          
            List<Vertex<String, CircleState>> vertices = buildTestVertices();
            List<Edge<String, String>> edges = buildTestEdges();
            
        
            com.alibaba.geaflow.api.graph.Graph<String, CircleState, String> graph = 
                graphEnv.buildGraph(
                    env.fromCollection(vertices, VertexUtils.<String, CircleState>getVertexTypeInfo()),
                    env.fromCollection(edges, EdgeUtils.<String, String>getEdgeTypeInfo())
                );
            
            // 执行单点环路检测
            String sourceNode = "A";
            int minCircleLength = 3;
            int maxCircleLength = 5;
            
            graph.pregel(
                "Single Vertex Circle Detection",
                new CircleState(sourceNode),
                new CircleComputeFunction(sourceNode, minCircleLength, maxCircleLength),
                new CircleMessagingFunction(),
                TimeWindows.of(TimeWindow.of(1000L))
            ).execute();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // 构建测试顶点数据
    private static List<Vertex<String, CircleState>> buildTestVertices() {
        List<Vertex<String, CircleState>> vertices = new ArrayList<>();
        String[] nodeIds = {"A", "B", "C", "D", "E", "F", "G"};
        
        for (String nodeId : nodeIds) {
            vertices.add(Vertex.<String, CircleState>of(nodeId, new CircleState("A")));
        }
        
        return vertices;
    }
    
 
    private static List<Edge<String, String>> buildTestEdges() {
        List<Edge<String, String>> edges = new ArrayList<>();
        
        // 添加边形成环路 ABCA
        edges.add(Edge.of("A", "B", "weight"));
        edges.add(Edge.of("B", "C", "weight"));
        edges.add(Edge.of("C", "A", "weight"));
        
        // 添加边形成环路 ADEA
        edges.add(Edge.of("A", "D", "weight"));
        edges.add(Edge.of("D", "E", "weight"));
        edges.add(Edge.of("E", "A", "weight"));
        
        // 添加非环路边
        edges.add(Edge.of("F", "G", "weight"));
        edges.add(Edge.of("G", "F", "weight"));
        
        return edges;
    }
    
    // 验证正确性
    public static class CircleDetectionTest {
        
        public void testSimpleCircleDetection() {
            try {
            
                GraphEnvironment graphEnv = GraphEnvironment.get();
                PregelExecutionEnvironment env = graphEnv.getPregelEnvironment();
                
                List<Vertex<String, CircleState>> vertices = Arrays.asList(
                    Vertex.of("A", new CircleState("A")),
                    Vertex.of("B", new CircleState("A"))
                );
                
                List<Edge<String, String>> edges = Arrays.asList(
                    Edge.of("A", "B", "weight"),
                    Edge.of("B", "A", "weight")
                );
                
              
                com.alibaba.geaflow.api.graph.Graph<String, CircleState, String> graph = 
                    graphEnv.buildGraph(
                        env.fromCollection(vertices, VertexUtils.<String, CircleState>getVertexTypeInfo()),
                        env.fromCollection(edges, EdgeUtils.<String, String>getEdgeTypeInfo())
                    );
                
           
                String sourceNode = "A";
                int minCircleLength = 3;
                int maxCircleLength = 5;
                
               
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                System.setOut(new PrintStream(baos));
                
                graph.pregel(
                    "Test Circle Detection",
                    new CircleState(sourceNode),
                    new CircleComputeFunction(sourceNode, minCircleLength, maxCircleLength),
                    new CircleMessagingFunction(),
                    TimeWindows.of(TimeWindow.of(1000L))
                ).execute();
                
                String output = baos.toString();
              
                Assert.assertTrue(!output.contains("找到环路"));
                
                baos = new ByteArrayOutputStream();
                System.setOut(new PrintStream(baos));
                
                graph.pregel(
                    "Test Circle Detection",
                    new CircleState(sourceNode),
                    new CircleComputeFunction(sourceNode, 2, 5),
                    new CircleMessagingFunction(),
                    TimeWindows.of(TimeWindow.of(1000L))
                ).execute();
                
                output = baos.toString();
                
                Assert.assertTrue(output.contains("找到环路（源节点：A）：A->B->A"));
                
                System.out.println("单元测试通过：简单环路检测");
                
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("单元测试失败：" + e.getMessage());
            }
        }
        
       
        static class Assert {
            static void assertTrue(boolean condition) {
                if (!condition) {
                    throw new AssertionError("断言失败");
                }
            }
            
            static void fail(String message) {
                throw new AssertionError(message);
            }
        }
    }
}



// private static List<Edge<String, String>> buildTestEdges() {
//     List<Edge<String, String>> edges = new ArrayList<>();
    
//     // 原环路：A→B→C→A（路径顺序1）
//     edges.add(Edge.of("A", "B", "weight"));
//     edges.add(Edge.of("B", "C", "weight"));
//     edges.add(Edge.of("C", "A", "weight"));
    
//     // 新增环路：A→C→B→A（路径顺序2，节点与原环路相同）
//     edges.add(Edge.of("A", "C", "weight"));
//     edges.add(Edge.of("C", "B", "weight"));
//     edges.add(Edge.of("B", "A", "weight"));
    
//     // 原环路：A→D→E→A
//     edges.add(Edge.of("A", "D", "weight"));
//     edges.add(Edge.of("D", "E", "weight"));
//     edges.add(Edge.of("E", "A", "weight"));
    
//     // 非环路边
//     edges.add(Edge.of("F", "G", "weight"));
//     edges.add(Edge.of("G", "F", "weight"));
    
//     return edges;
// }