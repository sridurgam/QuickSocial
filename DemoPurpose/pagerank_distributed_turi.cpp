#include <string>

#include <graphlab/graph/distributed_graph.hpp>
#include <graphlab/macros_def.hpp>

#define termination_bound 1e-5
#define damping_factor 0.85

struct vertex_data {
  float value;
  float self_weight;
  vertex_data(float value = 1) : value(value), self_weight(0) { }
};

struct edge_data {
  float weight;
  float old_source_value;
  edge_data(float weight) : weight(weight), old_source_value(0) { }
};

typedef graphlab::graph<vertex_data, edge_data> pagerank_graph;
typedef graphlab::types<pagerank_graph> gl_types;

void create_graph(pagerank_graph& graph) {
   graph.add_vertex(vertex_data());
   graph.add_vertex(vertex_data());
   graph.add_vertex(vertex_data());
   graph.add_vertex(vertex_data());
   graph.add_vertex(vertex_data());

  
   graph.add_edge(0, 3, edge_data(1));

  
   graph.add_edge(1, 0, edge_data(0.5));
   graph.add_edge(1, 2, edge_data(0.5));

   graph.add_edge(2, 0, edge_data(1.0/3));
   graph.add_edge(2, 1, edge_data(1.0/3));
   graph.add_edge(2, 3, edge_data(1.0/3));
   graph.add_edge(3, 0, edge_data(0.25));
   graph.add_edge(3, 1, edge_data(0.25));
   graph.add_edge(3, 2, edge_data(0.25));
   graph.add_edge(3, 4, edge_data(0.25));
   graph.add_edge(4, 0, edge_data(0.2));
   graph.add_edge(4, 1, edge_data(0.2));
   graph.add_edge(4, 2, edge_data(0.2));
   graph.add_edge(4, 3, edge_data(0.2));
   graph.vertex_data(4).self_weight = 0.2;
}

void pagerank_update(gl_types::iscope &scope, gl_types::icallback &scheduler) {
   vertex_data& vdata = scope.vertex_data();
   float sum = vdata.value * vdata.self_weight;

  foreach(graphlab::edge_id_t eid, scope.in_edge_ids()) {
    const vertex_data& neighbor_vdata =
      scope.const_neighbor_vertex_data(scope.source(eid));
    double neighbor_value = neighbor_vdata.value;

    edge_data& edata = scope.edge_data(eid);
    double contribution = edata.weight * neighbor_value;

    sum += contribution;

    edata.old_source_value = neighbor_value;
  }

  sum = (1-damping_factor)/scope.num_vertices() + damping_factor*sum;
  vdata.value = sum;

  foreach(graphlab::edge_id_t eid, scope.out_edge_ids()) {
    edge_data& outedgedata = scope.edge_data(eid);
  double residual =
      outedgedata.weight *
      std::fabs(outedgedata.old_source_value - vdata.value);
    if(residual > termination_bound) {
      gl_types::update_task task(scope.target(eid), pagerank_update);
      scheduler.add_task(task, residual);
    }
  }

  }

int main(int argc, char** argv) {
  global_logger().set_log_level(LOG_INFO);
  global_logger().set_log_to_console(true);
  logger(LOG_INFO, "PageRank starting\n");

  graphlab::command_line_options
    clopts("Run the PageRank algorithm.");

  gl_types::core core;

  if(!clopts.parse(argc, argv)) {
     std::cout << "Error in parsing input." << std::endl;
     return EXIT_FAILURE;
  }

  core.set_engine_options(clopts);

  create_graph(core.graph());

  core.add_task_to_all(pagerank_update, 100.0);

  double runtime = core.start();

  return EXIT_SUCCESS;
}
