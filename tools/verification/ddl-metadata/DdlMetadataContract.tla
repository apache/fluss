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
-------------------------- MODULE DdlMetadataContract --------------------------
EXTENDS Naturals, FiniteSets, Sequences
CONSTANTS Parts
VARIABLES live, partLive, tableId, partId, schema, history
vars == <<live, partLive, tableId, partId, schema, history>>
Init == /\ live = TRUE /\ partLive = [p \in Parts |-> TRUE]
        /\ tableId = 0 /\ partId = [p \in Parts |-> 0] /\ schema = 0 /\ history = <<>>
Record(op, t, p, partition) == [op |-> op, expectedTable |-> t, actualTable |-> tableId,
    expectedPart |-> p, actualPart |-> partId[partition], partition |-> partition]
Publish(op, t, p, partition) ==
    /\ (op = "create-table" \/ t = tableId)
    /\ (op \notin {"alter-part", "drop-part"} \/ p = partId[partition])
    /\ (CASE op = "alter-table" -> live
          [] op \in {"alter-part", "drop-part"} -> live /\ partLive[partition]
          [] op = "drop-table" -> live
          [] op = "create-table" -> ~live
          [] OTHER -> op = "create-part" /\ live /\ ~partLive[partition])
    /\ live' = IF op = "drop-table" THEN FALSE ELSE IF op = "create-table" THEN TRUE ELSE live
    /\ partLive' = CASE op = "drop-table" -> [q \in Parts |-> FALSE]
                    [] op = "create-table" -> [q \in Parts |-> TRUE]
                    [] op = "drop-part" -> [partLive EXCEPT ![partition] = FALSE]
                    [] op = "create-part" -> [partLive EXCEPT ![partition] = TRUE]
                    [] OTHER -> partLive
    /\ tableId' = IF op = "create-table" THEN tableId + 1 ELSE tableId
    /\ partId' = IF op = "create-table" THEN [q \in Parts |-> partId[q] + 1]
                  ELSE IF op = "create-part" THEN [partId EXCEPT ![partition] = @ + 1] ELSE partId
    /\ schema' = IF op = "create-table" THEN 0 ELSE IF op = "alter-table" THEN schema + 1 ELSE schema
    /\ history' = Append(history, Record(op, t, p, partition))
Next == IF Len(history') = Len(history) + 1
        THEN LET event == history'[Len(history')] IN
             Publish(event.op, event.expectedTable, event.expectedPart, event.partition)
        ELSE FALSE
Spec == Init /\ [][Next]_vars
NoWrongIncarnation == \A i \in 1..Len(history) :
    /\ (history[i].op = "create-table" \/ history[i].expectedTable = history[i].actualTable)
    /\ (history[i].op \notin {"alter-part", "drop-part"} \/ history[i].expectedPart = history[i].actualPart)
=============================================================================
