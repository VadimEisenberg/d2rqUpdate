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

package il.ac.technion.cs.d2rqUpdate.assembler;
import il.ac.technion.cs.d2rqUpdate.ModelD2RQUpdate;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.vocab.D2RQ;

/**
 * A Jena assembler that builds ModelD2RQUpdates.
 * 
 * @author Vadim Eisenberg Vadim.Eisenberg@gmail.com
 */
public class D2RQUpdateAssembler extends AssemblerBase {

	@Override
	public Object open(Assembler ignore, Resource description, Mode ignore2) {
		if (!description.hasProperty(D2RQ.mappingFile)) {
			throw new D2RQException("Error in assembler specification "
					+ description + ": missing property d2rq:mappingFile");
		}

		Statement mappingFile = description.getProperty(D2RQ.mappingFile);
		if (!mappingFile.getObject().isURIResource()) {
			throw new D2RQException("Error in assembler specification "
					+ description + ": value of d2rq:mappingFile must be a URI");
		}
		String mappingFileURI = ((Resource) mappingFile.getObject()).getURI();

		String resourceBaseURI = null;
		Statement stmt = description.getProperty(D2RQ.resourceBaseURI);
		if (stmt != null) {
			if (!stmt.getObject().isURIResource()) {
				throw new D2RQException("Error in assembler specification "
						+ description
						+ ": value of d2rq:resourceBaseURI must be a URI");
			}
			resourceBaseURI = ((Resource) stmt.getObject()).getURI();
		}
		return new ModelD2RQUpdate(mappingFileURI, null, resourceBaseURI);
	}
}
