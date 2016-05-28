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
 

import il.ac.technion.cs.d2rqUpdate.UpdateProcessor.UpdateProcessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.modify.UpdateProcessorRegistry;
import com.hp.hpl.jena.vocabulary.RDF;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.GraphD2RQ;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.sql.SQL;
import de.fuberlin.wiwiss.d2rq.values.Pattern;
import de.fuberlin.wiwiss.d2rq.vocab.D2RQ;

/**
 * An extension to de.fuberlin.wiwiss.d2rq.GraphD2RQ to enable update of the
 * D2RQ-mapped non-RDF database
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 * 
 * @see de.fuberlin.wiwiss.d2rq.GraphD2RQ
 */
public class GraphD2RQUpdate extends GraphD2RQ {
	static private Log log = LogFactory.getLog(GraphD2RQUpdate.class);
	
	static {
		UpdateProcessorRegistry.get().add(UpdateProcessor.getFactory());
	}
	
	private final Capabilities capabilities = new D2RQUpdateCapabilities();
	private Set<Attribute> mappedAttributes = Collections.emptySet();

	Set<Attribute> getMappedAttributes() {
		return mappedAttributes;
	}

	public GraphD2RQUpdate(Model mapModel, String baseURIForData)
			throws D2RQException {
		super(mapModel, baseURIForData);
		bulkHandler = new BulkUpdateHandler(this);
		mappedAttributes = getAllMappedAttributes(mapModel);
	}


	private Set<Attribute> getAllMappedAttributes(Model mapModel) {
		Set<Attribute> mappedAttributes = new HashSet<Attribute>();
		
		extractAttributesFromColumns(mapModel, mappedAttributes);
		extractAttributesFromPatterns(mapModel, mappedAttributes);
		extractAttributesFromJoins(mapModel, mappedAttributes);

		return mappedAttributes;
	}

	/**
	 * @param mapModel
	 * @param mappedAttributes2
	 */
	private void extractAttributesFromJoins(Model mapModel,
		Set<Attribute> mappedAttributes2) {
	}

	private void extractAttributesFromColumns(Model mapModel,
		Set<Attribute> mappedAttributes) {
		Query query = 
			QueryFactory.create("PREFIX d2rq: <" + D2RQ.NS + ">\n" +
					"PREFIX rdf: <" + RDF.getURI() + ">\n" +
					"SELECT ?column WHERE { " +
					"?propertyBridge rdf:type d2rq:PropertyBridge . " +
					"{ ?propertyBridge d2rq:column ?column } UNION " +
					"{ ?propertyBridge d2rq:uriColumn ?column }" +
					"}");
				
		QueryExecution queryExecution = 
			QueryExecutionFactory.create(query, mapModel);
	
		ResultSet resultSet = queryExecution.execSelect();
		while (resultSet.hasNext()) {
			QuerySolution solution = resultSet.nextSolution();
			Literal attributeLiteral = solution.getLiteral("column");
			if (attributeLiteral != null) {
				String attributeString = attributeLiteral.getString();
				mappedAttributes.add(SQL.parseAttribute(attributeString));
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void extractAttributesFromPatterns(Model mapModel,
		Set<Attribute> mappedAttributes) {
		Query query = 
			QueryFactory.create("PREFIX d2rq: <" + D2RQ.NS + ">\n" +
					"PREFIX rdf: <" + RDF.getURI() + ">\n" +
					"SELECT ?pattern WHERE { " +
					"?propertyBridge rdf:type d2rq:PropertyBridge . " +
					"{ ?propertyBridge d2rq:pattern ?pattern } UNION " +
					"{ ?propertyBridge d2rq:uriPattern ?pattern }" +
					"}");
				
		QueryExecution queryExecution = 
			QueryExecutionFactory.create(query, mapModel);
	
		ResultSet resultSet = queryExecution.execSelect();
		while (resultSet.hasNext()) {
			QuerySolution solution = resultSet.nextSolution();
			Literal patternLiteral = solution.getLiteral("pattern");
			if (patternLiteral != null) {
				String patternString = patternLiteral.getString();
				Pattern pattern = new Pattern(patternString);
				mappedAttributes.addAll(pattern.attributes());
			}
		}
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void performAdd(Triple triple) {
		log.debug("adding triple: " + triple.getSubject() + " "
				+ triple.getPredicate() + " " + triple.getObject());
		new TripleAdder(Collections.singleton(triple), 
				tripleRelations()).execute();

	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void performDelete(Triple triple) {
		log.debug("deleting triple: " + triple.getSubject() + " "
				+ triple.getPredicate() + " " + triple.getObject());
		new TripleDeleter(Collections.singleton(triple), 
				tripleRelations(),getMappedAttributes()).execute();

	}
	
	@Override
	public Capabilities getCapabilities() { 
		return this.capabilities;
	}
}
