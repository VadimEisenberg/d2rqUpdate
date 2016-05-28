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

import com.hp.hpl.jena.shared.JenaException;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 * The code of the class is the same as de.fuberlin.wiwiss.d2rq.D2RQException
 * The error codes are different
 */
public class D2RQUpdateException extends JenaException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final int UNSPECIFIED = 0;
	public static final int NOT_IMPLEMENTED = 1;
	public static final int SQL_STATEMENT_FAILED = 2;
	public static final int DELETE_NOT_NULLABLE_ATTRIBUTE = 3;

	private final int code;

	public D2RQUpdateException(String message) {
		this(message, UNSPECIFIED);
	}

	public D2RQUpdateException(Throwable cause) {
		this(cause, UNSPECIFIED);
	}

	public D2RQUpdateException(String message, Throwable cause) {
		this(message, cause, UNSPECIFIED);
	}

	public D2RQUpdateException(String message, int code) {
		super(message + " (E" + code + ")");
		this.code = code;
	}

	public D2RQUpdateException(Throwable cause, int code) {
		super(cause);
		this.code = code;
	}

	public D2RQUpdateException(String message, Throwable cause, int code) {
		super(message + " (E" + code + ")", cause);
		this.code = code;
	}

	public int errorCode() {
		return this.code;
	}
}
