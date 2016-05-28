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
import com.hp.hpl.jena.enhanced.BuiltinPersonalities;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;
import com.hp.hpl.jena.util.FileManager;

/**
 * <p>
 * A D2RQUpdate read-write Jena model backed by a D2RQ-mapped non-RDF database.
 * </p>
 * 
 * <p>
 * D2RQ is a declarative mapping language for describing mappings between
 * ontologies and relational data models. More information about D2RQ is found
 * at: http://www4.wiwiss.fu-berlin.de/bizer/d2rq/
 * </p>
 * 
 * <p>
 * This class is a thin wrapper around a {@link GraphD2RQUpdate} and provides
 * only convenience constructors.
 * </p>
 * 
 * This class does not extend ModelD2RQ, since there is no constructor of 
 * ModelD2RQ that receives a graph as a parameter. Otherwise, it would be 
 * possible to pass GraphD2RQUpdate as a parameter to ModelD2RQ
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 * 
 * @see il.ac.technion.cs.d2rqUpdate.GraphD2RQUpdate
 * @see de.fuberlin.wiwiss.d2rq.ModelD2RQ
 */
public class ModelD2RQUpdate extends ModelCom implements Model {

	/** 
	 * Create a non-RDF database-based model. The model is created
	 * from a D2RQ map that will be loaded from the given URL.
	 * Its serialization format will be guessed from the
	 * file extension and defaults to RDF/XML.
	 * @param mapURL URL of the D2RQ map to be used for this model
	 */
	public ModelD2RQUpdate(String mapURL) {
		this(FileManager.get().loadModel(mapURL), mapURL + "#");
	}

	/** 
	 * Create a non-RDF database-based model. The model is created
	 * from a D2RQ map that may be in "RDF/XML", "N-TRIPLES" or "N3"
	 * format.
	 * @param mapURL URL of the D2RQ map to be used for this model
	 * @param serializationFormat the format of the map, or <tt>null</tt>
	 * 		for guessing based on the file extension
	 * @param baseURIForData Base URI for turning relative URI patterns into
	 * 		absolute URIs; if <tt>null</tt>, then D2RQ will pick a base URI
	 */
	public ModelD2RQUpdate(String mapURL, String serializationFormat,
			String baseURIForData) {
		this(FileManager.get().loadModel(mapURL, serializationFormat),
				(baseURIForData == null) ? mapURL + "#" : baseURIForData);
	}

	/** 
	 * Create a non-RDF database-based model. The model is created
	 * from a D2RQ map that is provided as a Jena model.
	 * @param mapModel a Jena model containing the D2RQ map
	 * @param baseURIForData Base URI for turning relative URI patterns into
	 * 		absolute URIs; if <tt>null</tt>, then D2RQ will pick a base URI
	 */
	public ModelD2RQUpdate(Model mapModel, String baseURIForData) {
		super(new GraphD2RQUpdate(mapModel, baseURIForData),
		// BuiltinPersonalities.model really required?
				BuiltinPersonalities.model);
	}
}