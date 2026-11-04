(*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)
---------------------------- MODULE DdlGateContract ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Calls, DatabaseCall, DatabaseOf, TableOf
VARIABLES context, leases, phase, captured
vars == <<context, leases, phase, captured>>
Conflict(a, b) == a # b /\ DatabaseOf[a] = DatabaseOf[b]
    /\ (a = DatabaseCall \/ b = DatabaseCall \/ TableOf[a] = TableOf[b])
Busy(a) == \E b \in leases : captured[b] = context /\ Conflict(a, b)
Init == /\ context = 0 /\ leases = {}
        /\ phase = [a \in Calls |-> "new"] /\ captured = [a \in Calls |-> 0]
Acquire(a) == /\ phase[a] = "new" /\ ~Busy(a)
              /\ leases' = leases \cup {a} /\ phase' = [phase EXCEPT ![a] = "held"]
              /\ captured' = [captured EXCEPT ![a] = context] /\ UNCHANGED context
Reject(a) == /\ phase[a] = "new" /\ Busy(a)
             /\ phase' = [phase EXCEPT ![a] = "rejected"]
             /\ UNCHANGED <<context, leases, captured>>
Close(a) == /\ phase[a] = "held" /\ leases' = leases \ {a}
            /\ phase' = [phase EXCEPT ![a] = "closed"]
            /\ UNCHANGED <<context, captured>>
Rotate == /\ context = 0 /\ context' = 1 /\ UNCHANGED <<leases, phase, captured>>
Next == Rotate \/ (\E a \in Calls : Acquire(a) \/ Reject(a) \/ Close(a))
Spec == Init /\ [][Next]_vars
Exclusive == \A a, b \in leases : captured[a] = captured[b] => ~Conflict(a, b)
=============================================================================
