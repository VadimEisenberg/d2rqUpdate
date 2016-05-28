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

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.modify.UpdateProcessorFactory;
import com.hp.hpl.jena.sparql.modify.UpdateVisitor;
import com.hp.hpl.jena.sparql.modify.op.Update;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateRequest;


/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class UpdateProcessor implements com.hp.hpl.jena.update.UpdateProcessor {
	
	private final GraphStore graphStore;
	private final UpdateRequest request;
	private final Binding inputBinding;

	private UpdateProcessor(GraphStore graphStore, UpdateRequest request,
			Binding inputBinding) {
		this.graphStore = graphStore;
		this.request = request;
		this.inputBinding = inputBinding;
	}

	public void execute() {
		graphStore.startRequest();
		UpdateVisitor v = 
			new 
			il.ac.technion.cs.d2rqUpdate.UpdateProcessor.UpdateProcessorVisitor(
													  graphStore, inputBinding);
		for (Update update : request.getUpdates()) {
			update.visit(v);
		}
		graphStore.finishRequest();
	}

	public static UpdateProcessorFactory getFactory() {
		return new UpdateProcessorFactory() {
			public boolean accept(UpdateRequest request, 
				GraphStore graphStore) {
				return true;
			}

			public UpdateProcessor create(UpdateRequest request,
				GraphStore graphStore, Binding inputBinding) {
				return new 
				il.ac.technion.cs.d2rqUpdate.UpdateProcessor.UpdateProcessor(
											 graphStore, request, inputBinding);
			}
		};
	}
}
