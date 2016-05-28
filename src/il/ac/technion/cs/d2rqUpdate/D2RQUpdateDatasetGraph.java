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

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.update.GraphStore;

import de.fuberlin.wiwiss.d2rq.GraphD2RQ;
import de.fuberlin.wiwiss.d2rq.engine.D2RQDatasetGraph;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */

@SuppressWarnings("unchecked")
public class D2RQUpdateDatasetGraph extends D2RQDatasetGraph 
	implements GraphStore {

	/**
	 * @param graph
	 */
	public D2RQUpdateDatasetGraph(GraphD2RQ graph) {
		super(graph);
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.update.GraphStore#finishRequest()
	 */
	@Override
	public void finishRequest() {	
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.update.GraphStore#startRequest()
	 */
	@Override
	public void startRequest() {
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.update.GraphStore#toDataset()
	 */
	@Override
	public Dataset toDataset() {
		return new DatasetImpl(this);
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.sparql.core.DataSourceGraph#addGraph(com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Graph)
	 */
	@Override
	public void addGraph(Node arg0, Graph arg1) {
		// Do nothing
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.sparql.core.DataSourceGraph#removeGraph(com.hp.hpl.jena.graph.Node)
	 */
	@Override
	public Graph removeGraph(Node arg0) {
		// do nothing
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hp.hpl.jena.sparql.core.DataSourceGraph#setDefaultGraph(com.hp.hpl.jena.graph.Graph)
	 */
	@Override
	public void setDefaultGraph(Graph arg0) {
		// do nothing
	}



	
}
