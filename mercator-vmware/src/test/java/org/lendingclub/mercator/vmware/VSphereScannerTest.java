/**
 * Copyright 2017-2018 LendingClub, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lendingclub.mercator.vmware;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;
import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.vmware.VMWareScanner;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmware.vim25.ManagedObjectReference;

public class VSphereScannerTest {

	ObjectMapper mapper = new ObjectMapper();

	/*
	 * @Test public void testCreateSetClause() { VSphereScanner scanner = new
	 * VSphereScanner();
	 * 
	 * String clause = scanner.createSetClause("x", mapper.createObjectNode()
	 * .put("a", "1").put("foo", "bar"));
	 * 
	 * assertThat(clause).contains("x.a={a}").contains("x.foo={foo}")
	 * .contains(",");
	 * 
	 * }
	 */
	@Test
	public void testComputeMacId() {
		ManagedObjectReference mor = new ManagedObjectReference();
		mor.setType("HostSystem");
		mor.setVal("host-123");

		VMWareScanner s = Mockito.mock(VMWareScanner.class);
		when(s.getVCenterId()).thenReturn("abcdef");
		Mockito.when(s.computeUniqueId(mor)).thenCallRealMethod();

		assertThat(mor.getType()).isEqualTo("HostSystem");
		assertThat(s.computeUniqueId(mor)).isEqualTo("21b23eae3d48797d8d057329705825e637e35d81");

		VMWareScanner s2 = Mockito.mock(VMWareScanner.class);

		when(s2.getVCenterId()).thenReturn("another");
		Mockito.when(s2.computeUniqueId(mor)).thenCallRealMethod();
		assertThat(s.computeUniqueId(mor)).isNotEqualTo(s2.computeUniqueId(mor));
		/*
		 * try { new VSphereScanner().computeMacId(null); } catch (Exception e)
		 * { assertThat(e) .isExactlyInstanceOf(NullPointerException.class)
		 * .hasMessageContaining("cannot be null"); }
		 * 
		 * mor = new ManagedObjectReference(); mor.setType("VirtualMachine");
		 * mor.setVal("vm-123"); try { new VSphereScanner().computeMacId(mor);
		 * fail(); } catch (Exception e) { assertThat(e).isInstanceOf(
		 * IllegalArgumentException.class); }
		 */
	}



}
