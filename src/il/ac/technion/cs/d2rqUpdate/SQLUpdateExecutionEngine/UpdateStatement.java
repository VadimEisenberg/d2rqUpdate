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


package il.ac.technion.cs.d2rqUpdate.SQLUpdateExecutionEngine;

import java.util.Map;

import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class UpdateStatement {
	
	
	@Override
	public String toString() {
		return "UpdateStatement [database=" + database
				+ ", minimalNumberOfUpdatedRows=" + minimalNumberOfUpdatedRows
				+ ", sqlString=" + sqlString + ", table=" + table
				+ ", updateType=" + updateType + ", updatedAttributes="
				+ updatedAttributes + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result =
				prime * result + ((database == null) ? 0 : database.hashCode());
		result = prime * result + minimalNumberOfUpdatedRows;
		result =
				prime * result
						+ ((sqlString == null) ? 0 : sqlString.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result =
				prime * result
						+ ((updateType == null) ? 0 : updateType.hashCode());
		result =
				prime
						* result
						+ ((updatedAttributes == null) ? 0 : updatedAttributes
								.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		UpdateStatement other = (UpdateStatement) obj;
		if (database == null) {
			if (other.database != null) {
				return false;
			}
		} else if (!database.equals(other.database)) {
			return false;
		}
		if (minimalNumberOfUpdatedRows != other.minimalNumberOfUpdatedRows) {
			return false;
		}
		if (sqlString == null) {
			if (other.sqlString != null) {
				return false;
			}
		} else if (!sqlString.equals(other.sqlString)) {
			return false;
		}
		if (table == null) {
			if (other.table != null) {
				return false;
			}
		} else if (!table.equals(other.table)) {
			return false;
		}
		if (updateType == null) {
			if (other.updateType != null) {
				return false;
			}
		} else if (!updateType.equals(other.updateType)) {
			return false;
		}
		if (updatedAttributes == null) {
			if (other.updatedAttributes != null) {
				return false;
			}
		} else if (!updatedAttributes.equals(other.updatedAttributes)) {
			return false;
		}
		return true;
	}

	public enum Type {
		INSERT,

		UPDATE_TO_NON_NULL_VALUE {
			@Override
			public String toString() {
				return "UPDATE TO NON NULL VALUE";
			}
		},

		UPDATE_TO_NULL {
			@Override
			public String toString() {
				return "UPDATE TO NULL";
			}
		},

		DELETE
	}
	
	private final ConnectedDB database;
	private final RelationName table;
	private final Map<Attribute,String> updatedAttributes;
	private final String sqlString;
	private final int minimalNumberOfUpdatedRows;
	private final Type updateType;
	private final Map<Attribute, String> subjectValues;

	
	public UpdateStatement(ConnectedDB database,
		RelationName table,
		Map<Attribute,String> updatedAttributes, String sqlString,
		int minimalNumberOfUpdatedRows, 
		Type updateType,
		Map<Attribute, String> subjectValues) {
		this.database = database;
		this.table = table;
		this.updatedAttributes = updatedAttributes;
		this.sqlString = sqlString;
		this.minimalNumberOfUpdatedRows = minimalNumberOfUpdatedRows;
		this.updateType = updateType;
		this.subjectValues = subjectValues;
	}

	public Map<Attribute,String> getUpdatedAttributes() {
		return updatedAttributes;
	}

	public String getSqlString() {
		return sqlString;
	}

	public ConnectedDB getDatabase() {
		return database;
	}



	public int getMinimalNumberOfUpdatedRows() {
		return minimalNumberOfUpdatedRows;
	}

	public RelationName getTable() {
		return table;
	}

	public Type getUpdateType() {
		return updateType;
	}

	/**
	 * @return
	 */
	public Map<Attribute, String> getSubjectValues() {
		return subjectValues;
	}
}
