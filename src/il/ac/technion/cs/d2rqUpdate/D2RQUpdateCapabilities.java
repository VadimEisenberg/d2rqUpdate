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

import com.hp.hpl.jena.graph.Capabilities;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class D2RQUpdateCapabilities implements Capabilities {

	public boolean sizeAccurate() {
		return true;
	}

	public boolean addAllowed() {
		return addAllowed(false);
	}

	public boolean addAllowed(boolean every) {
		return !every;
	}

	public boolean deleteAllowed() {
		return deleteAllowed(false);
	}

	public boolean deleteAllowed(boolean every) {
		return !every;
	}

	public boolean canBeEmpty() {
		return true;
	}

	public boolean iteratorRemoveAllowed() {
		return false;
	}

	public boolean findContractSafe() {
		return false;
	}

	public boolean handlesLiteralTyping() {
		return true;
	}
}

