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
------------------------------ MODULE DdlAdmission ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Scenario, ProtectDatabase, ProtectTable, Cleanup
VARIABLES context, phase, captured, tokenState, entries, stateId, exclusive, tables
vars == <<context, phase, captured, tokenState, entries, stateId, exclusive, tables>>
Calls == {"a", "b", "db"}
Databases == {"x", "y"}
TableNames == {"first", "second"}
Keys == (0..1) \X Databases
Database(a) == IF a = "b" /\ Scenario = "other-database" THEN "y" ELSE "x"
Table(a) == IF a = "b" /\ Scenario = "other-table" THEN "second" ELSE "first"
Key(a) == <<captured[a], Database(a)>>
Held == {a \in Calls : phase[a] = "held"}
Busy(a) == LET k == <<context, Database(a)>> IN
    k \in entries /\ (exclusive[k] \/ (ProtectDatabase /\ a = "db")
        \/ (ProtectTable /\ a # "db" /\ Table(a) \in tables[k]))
Init == /\ context = 0 /\ phase = [a \in Calls |-> "new"]
        /\ captured = [a \in Calls |-> 0] /\ tokenState = [a \in Calls |-> 0]
        /\ entries = {} /\ stateId = [k \in Keys |-> 0]
        /\ exclusive = [k \in Keys |-> FALSE] /\ tables = [k \in Keys |-> {}]
Acquire(a) ==
    /\ phase[a] = "new"
    /\ (LET k == <<context, Database(a)>> IN
       IF Busy(a)
       THEN /\ phase' = [phase EXCEPT ![a] = "rejected"]
            /\ UNCHANGED <<captured, tokenState, entries, stateId, exclusive, tables>>
       ELSE /\ phase' = [phase EXCEPT ![a] = "held"]
            /\ captured' = [captured EXCEPT ![a] = context]
            /\ tokenState' = [tokenState EXCEPT ![a] = stateId[k] + IF k \in entries THEN 0 ELSE 1]
            /\ stateId' = [stateId EXCEPT ![k] = @ + IF k \in entries THEN 0 ELSE 1]
            /\ entries' = entries \cup {k}
            /\ exclusive' = [exclusive EXCEPT ![k] = @ \/ a = "db"]
            /\ tables' = IF a = "db" THEN tables ELSE [tables EXCEPT ![k] = @ \cup {Table(a)}])
    /\ UNCHANGED context
Close(a) ==
    /\ phase[a] = "held" /\ phase' = [phase EXCEPT ![a] = "closed"]
    /\ LET k == Key(a) IN
       /\ exclusive' = [exclusive EXCEPT ![k] = @ /\ a # "db"]
       /\ tables' = IF a = "db" THEN tables ELSE [tables EXCEPT ![k] = @ \ {Table(a)}]
       /\ entries' = IF Cleanup /\ ~exclusive'[k] /\ tables'[k] = {} /\ stateId[k] = tokenState[a]
                      THEN entries \ {k} ELSE entries
    /\ UNCHANGED <<context, captured, tokenState, stateId>>
CloseAgain(a) == /\ phase[a] = "closed" /\ UNCHANGED vars
Rotate == /\ context = 0 /\ context' = 1
          /\ UNCHANGED <<phase, captured, tokenState, entries, stateId, exclusive, tables>>
Step(a) == Acquire(a) \/ Close(a) \/ CloseAgain(a)
Finished == \A a \in Calls : phase[a] \in {"closed", "rejected"}
Next == Rotate \/ (\E a \in Calls : Step(a)) \/ (Finished /\ UNCHANGED vars)
Spec == Init /\ [][Next]_vars /\ (\A a \in Calls : WF_vars(Step(a)))
C == INSTANCE DdlGateContract WITH Calls <- Calls, DatabaseCall <- "db",
     DatabaseOf <- [a \in Calls |-> Database(a)], TableOf <- [a \in Calls |-> Table(a)],
     context <- context, leases <- Held, phase <- phase, captured <- captured
Refines == C!Spec
Exclusive == C!Exclusive
Accounting == /\ \A a \in Held : Key(a) \in entries /\ tokenState[a] = stateId[Key(a)]
              /\ \A k \in Keys :
                  /\ exclusive[k] <=> (\E a \in Held : a = "db" /\ Key(a) = k)
                  /\ tables[k] = {Table(a) : a \in {b \in Held : b # "db" /\ Key(b) = k}}
NoEmptyEntries == entries = {k \in Keys : exclusive[k] \/ tables[k] # {}}
EntryBound == Cardinality(entries) <= Cardinality(Held)
Progress == <>Finished
EventuallyEmpty == <>[] (entries = {})
NeverParallelTables == ~("a" \in Held /\ "b" \in Held /\ captured["a"] = captured["b"] /\ Table("a") # Table("b"))
NeverReallocated == \A k \in Keys : stateId[k] <= 1
=============================================================================
