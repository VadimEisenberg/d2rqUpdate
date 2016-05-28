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

 
package il.ac.technion.cs.d2rqUpdate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.SimpleBulkUpdateHandler;


/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 * 
 */
public class BulkUpdateHandler extends SimpleBulkUpdateHandler implements
	BulkUpdateHandlerWithDeleteInsert {
	static private Log log = LogFactory.getLog(BulkUpdateHandler.class);
	
	private final List<Triple> triplesToAdd = new ArrayList<Triple>();
	private final List<Triple> triplesToDelete = new ArrayList<Triple>();
	private boolean insideDeleteInsert = false;
	
	public void startDeleteInsertOperation() {
		triplesToAdd.clear();
		triplesToDelete.clear();
		insideDeleteInsert = true;
	}
	
	public void completeDeleteInsertOperation() {
		performDeleteAdd(triplesToDelete, triplesToAdd);
		triplesToAdd.clear();
		triplesToDelete.clear();
		insideDeleteInsert = false;
	}
	
	
	/**
	 * @param triplesToDelete
	 * @param triplesToAdd
	 */
	@SuppressWarnings("unchecked")
	private void performDeleteAdd(List<Triple> triplesToDelete,
		List<Triple> triplesToAdd) {
		
		log.debug("performDeleteAdd called");
		for (Triple triple : triplesToDelete) {
			log.debug("\t delete:" + triple);
		}
		for (Triple triple : triplesToAdd) {
			log.debug("\t add:" + triple);
		}
		
		new  TripleDeleteAddUpdater(triplesToDelete, triplesToAdd,
				((GraphD2RQUpdate)graph).tripleRelations(),
				((GraphD2RQUpdate)graph).getMappedAttributes()).execute();
	}

	/**
	 * @param graph
	 */
	public BulkUpdateHandler(GraphD2RQUpdate graph) {
		super(graph);	
	}

	@Override
	public void add(Triple[] triples) {
		performAdd(Arrays.asList(triples));
		manager.notifyAddArray(graph, triples);
	}

	@Override
	protected void add(List<Triple> triples, boolean notify) {
		performAdd(triples);
		if (notify) {
			manager.notifyAddList(graph, triples);
		}
	}

	@Override
	public void add(Graph g, boolean withReifications) {
		addIterator(GraphUtil.findAll(g), false);
		if (withReifications) {
			addReifications(graph, g);
		}
		manager.notifyAddGraph(graph, g);
	}

	@SuppressWarnings("unchecked")
	protected void performAdd(List<Triple> triples) {
		if (insideDeleteInsert) { 
			// do not perform add - just remember the triples to add
			triplesToAdd.addAll(triples);
			return;
		}
		new TripleAdder(triples, 
				((GraphD2RQUpdate)graph).tripleRelations()).execute();		
		
	}
	
	@Override
	public void delete(Triple[] triples) {
		performDelete(Arrays.asList(triples));
		manager.notifyDeleteArray(graph, triples);
	}

	@Override
	protected void delete(List<Triple> triples, boolean notify) {
		performDelete(triples);
		if (notify) {
			manager.notifyDeleteList(graph, triples);
		}
	}

	@Override
	public void delete(Graph g, boolean withReifications) {
		deleteIterator(GraphUtil.findAll(g), false);
		if (withReifications) {
			deleteReifications(graph, g);
		}
		manager.notifyDeleteGraph(graph, g);
	}

	@SuppressWarnings("unchecked")
	protected void performDelete(List<Triple> triples) {
		filterOutNonExistentTriples(triples, (GraphD2RQUpdate)graph);
		if (insideDeleteInsert) { 
			// do not perform delete - just remember the triples to delete
			triplesToDelete.addAll(triples);
			return;
		}
		new TripleDeleter(triples, 
				((GraphD2RQUpdate)graph).tripleRelations(),
				((GraphD2RQUpdate)graph).getMappedAttributes()).execute();		
		
	}

	/**
	 * @param triples
	 * @param graph
	 */
	private void filterOutNonExistentTriples(List<Triple> triples,
		GraphD2RQUpdate graph) {
		List<Triple> existentTriples = new LinkedList<Triple>();
		
		for (Triple triple : triples) {
			if (graph.contains(triple)) {
				existentTriples.add(triple);
			}
		}
		triples.retainAll(existentTriples);
	}
}
