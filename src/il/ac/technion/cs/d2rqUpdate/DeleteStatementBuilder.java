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

import java.util.Map;

import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * Builds DELETE SQL statement for a given relation, database and condition
 * values
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 */
public class DeleteStatementBuilder extends StatementBuilder {
	private final Map<Attribute, String> conditionValues;

	public DeleteStatementBuilder(RelationName table,
			ConnectedDB database, Map<Attribute, String> conditionValues) {
		super(table, database);
		this.conditionValues = conditionValues;
	}
	
	@Override
	public String getSQLStatement() {
		StringBuffer result = new StringBuffer("DELETE ");
		
		result.append(" FROM ");
		result.append(quoteRelationName(getTable()));

		result.append(" WHERE ");
		appendSeparatedEqualities(result, conditionValues, "AND", true);

		return result.toString();
	}
}
