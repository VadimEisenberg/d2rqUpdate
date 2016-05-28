/*
   Copyright 2010 Technion - Israel Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

 
package il.ac.technion.cs.d2rqUpdate.UpdateProcessor;

import il.ac.technion.cs.d2rqUpdate.BulkUpdateHandlerWithDeleteInsert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.modify.op.UpdateModify;
import com.hp.hpl.jena.update.GraphStore;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class UpdateProcessorVisitor extends
		com.hp.hpl.jena.sparql.modify.UpdateProcessorVisitor {
	
	static private Log log = 
		LogFactory.getLog(
	 il.ac.technion.cs.d2rqUpdate.UpdateProcessor.UpdateProcessorVisitor.class);
	
	private final GraphStore graphStore;
	
	/**
	 * @param graphStore
	 * @param initialBinding
	 */
	public UpdateProcessorVisitor(GraphStore graphStore, 
		Binding initialBinding) {
		super(graphStore, initialBinding);
		this.graphStore = graphStore;
	}
	

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.sparql.modify.UpdateProcessorVisitor#visit(com.hp.hpl.jena.sparql.modify.op.UpdateModify)
	 */
	@Override
	public void visit(UpdateModify modify) {
		log.debug("inside D2RQUpdate visitor");
		List<BulkUpdateHandlerWithDeleteInsert> 
			bulkUpdateHandlersWithDeleteInsert =
				getBulkUpdateHandlersWithDeleteInsert(modify);
		
		startDeleteInsertOperation(bulkUpdateHandlersWithDeleteInsert);
		
		super.visit(modify);
		
		completeDeleteInsertOperation(bulkUpdateHandlersWithDeleteInsert);
	}


	private void completeDeleteInsertOperation(
		List<BulkUpdateHandlerWithDeleteInsert> 
		bulkUpdateHandlersWithDeleteInsert) {
		for(BulkUpdateHandlerWithDeleteInsert bulkUpdateHandler : 
			bulkUpdateHandlersWithDeleteInsert) {
			bulkUpdateHandler.completeDeleteInsertOperation();
		}
	}


	private void startDeleteInsertOperation(
		List<BulkUpdateHandlerWithDeleteInsert> 
		bulkUpdateHandlersWithDeleteInsert) {
		for(BulkUpdateHandlerWithDeleteInsert bulkUpdateHandler : 
			bulkUpdateHandlersWithDeleteInsert) {
			bulkUpdateHandler.startDeleteInsertOperation();
		}
	}


	private List<BulkUpdateHandlerWithDeleteInsert> 
		getBulkUpdateHandlersWithDeleteInsert(UpdateModify modify) {
		List<BulkUpdateHandlerWithDeleteInsert> bulkUpdateHandlers = 
			new ArrayList<BulkUpdateHandlerWithDeleteInsert>(
					modify.getGraphNames().size());
		
		Set<Graph> updatedGraphs = getUpdatedGraphs(modify);
		
		for(Graph graph : updatedGraphs) {
			com.hp.hpl.jena.graph.BulkUpdateHandler bulkUpdateHandler = 
				graph.getBulkUpdateHandler();
			if (bulkUpdateHandler instanceof BulkUpdateHandlerWithDeleteInsert){
				bulkUpdateHandlers.add(
						(BulkUpdateHandlerWithDeleteInsert)bulkUpdateHandler);
			}
		}
		return bulkUpdateHandlers;
	}


	private Set<Graph> getUpdatedGraphs(UpdateModify modify) {
		Set<Graph> updatedGraphs = 
			new HashSet<Graph>(modify.getGraphNames().size() + 1);
		
		for(Node graphName : modify.getGraphNames()) {
			updatedGraphs.add(graphStore.getGraph(graphName));
		}
		
		if(updatedGraphs.isEmpty()){
			updatedGraphs.add(graphStore.getDefaultGraph());
		}
		return updatedGraphs;
	}
}
