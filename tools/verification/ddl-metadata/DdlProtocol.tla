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
------------------------------- MODULE DdlProtocol -------------------------------
EXTENDS Naturals, FiniteSets, Sequences
CONSTANTS Actors, Target, Mode, Focus, CachedInput, VerifyInput, AllowFailover, AllowReconnect
VARIABLES live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
          epoch, replaced, pc, held, context, staleAtEntry, tableRead, partRead,
          schemaRead, rootRead, versionRead, order, requestSession, processed, result,
          failed, lateWrite, staleExternal
vars == <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
          epoch, replaced, pc, held, context, staleAtEntry, tableRead, partRead,
          schemaRead, rootRead, versionRead, order, requestSession, processed, result,
          failed, lateWrite, staleExternal>>
Parts == {"p", "q"}
Partition == "p"
Creates == Actors \cap {"R", "S", "x"}
Drops == Actors \cap {"r", "s"}
Requests == Actors \X {"barrier", "mutation"}
Op(a) == IF a = "d" THEN (IF Target = "table" THEN "alter-table" ELSE "alter-part")
         ELSE IF a \in Drops THEN (IF Target = "table" THEN "drop-table" ELSE "drop-part")
         ELSE IF Target = "table" THEN "create-table" ELSE "create-part"
Sent == {order[i] : i \in 1..Len(order)}
Session(a) == IF Mode = "rebind" /\ context[a] = 0 /\ replaced THEN 1 ELSE context[a]
SessionValid(a) == Mode = "rebind" \/ context[a] # 0 \/ ~replaced
Before(r) == \A i, j \in 1..Len(order) : order[j] = r /\ i < j
    /\ requestSession[order[i]] = requestSession[r] => order[i] \in processed
ReadsOrdered(a) == \A r \in Sent : requestSession[r] = Session(a) => r \in processed
Eligible(a) ==
    IF Focus = "external" THEN epoch = 1
    ELSE CASE a = "d" -> IF CachedInput \/ Focus = "current" THEN pc["S"] = "done" ELSE TRUE
         [] a = "r" -> IF "d" \notin Actors \/ CachedInput \/ Focus = "current" THEN TRUE
                        ELSE pc["d"] \in {"send", "wait", "done"}
         [] a \in {"R", "x"} -> pc["r"] = "done"
         [] a = "s" -> pc["R"] = "done" /\ (IF "x" \in Actors THEN pc["x"] = "done" ELSE TRUE)
         [] OTHER -> pc["s"] = "done"
Busy(a) == \E b \in held : context[b] = (IF a = "d" THEN 0 ELSE epoch) /\ Mode # "shared"
Init ==
    /\ live = TRUE /\ partLive = [p \in Parts |-> TRUE] /\ tableId = 0
    /\ partId = [p \in Parts |-> 0] /\ schema = 0 /\ rootVersion = 1
    /\ partVersion = [p \in Parts |-> 0] /\ history = <<>> /\ epoch = 0 /\ replaced = FALSE
    /\ pc = [a \in Actors |-> "new"] /\ held = {} /\ context = [a \in Actors |-> 0]
    /\ staleAtEntry = [a \in Actors |-> FALSE] /\ tableRead = [a \in Actors |-> 0]
    /\ partRead = [a \in Actors |-> 0] /\ schemaRead = [a \in Actors |-> 0]
    /\ rootRead = [a \in Actors |-> 0] /\ versionRead = [a \in Actors |-> 0]
    /\ order = <<>> /\ requestSession = [r \in Requests |-> 0] /\ processed = {}
    /\ result = [r \in Requests |-> "none"] /\ failed = {} /\ lateWrite = FALSE /\ staleExternal = FALSE
Acquire(a) ==
    /\ pc[a] = "new" /\ Eligible(a)
    /\ pc' = [pc EXCEPT ![a] = IF Busy(a) THEN "done" ELSE "start"]
    /\ context' = [context EXCEPT ![a] = IF a = "d" THEN 0 ELSE epoch]
    /\ staleAtEntry' = [staleAtEntry EXCEPT ![a] = (a = "d" /\ epoch # 0)]
    /\ held' = IF Busy(a) THEN held ELSE held \cup {a}
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, tableRead, partRead, schemaRead, rootRead, versionRead,
                    order, requestSession, processed, result, failed, lateWrite, staleExternal>>
Start(a) ==
    /\ pc[a] = "start"
    /\ pc' = [pc EXCEPT ![a] = IF ~SessionValid(a) THEN "done"
        ELSE IF Mode = "no-barrier" THEN (IF Focus = "external" THEN "external" ELSE "read") ELSE "barrier-send"]
    /\ held' = IF SessionValid(a) THEN held ELSE held \ {a}
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, context, staleAtEntry, tableRead, partRead, schemaRead,
                    rootRead, versionRead, order, requestSession, processed, result, failed, lateWrite, staleExternal>>
Send(a, kind) ==
    /\ pc[a] = (IF kind = "barrier" THEN "barrier-send" ELSE "send")
    /\ SessionValid(a)
    /\ pc' = [pc EXCEPT ![a] = IF kind = "barrier" THEN "barrier-wait" ELSE "wait"]
    /\ order' = Append(order, <<a, kind>>)
    /\ requestSession' = [requestSession EXCEPT ![<<a, kind>>] = Session(a)]
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, held, context, staleAtEntry, tableRead, partRead,
                    schemaRead, rootRead, versionRead, processed, result, failed, lateWrite, staleExternal>>
Reply(a, kind) ==
    /\ pc[a] = (IF kind = "barrier" THEN "barrier-wait" ELSE "wait")
    /\ <<a, kind>> \in processed
    /\ pc' = [pc EXCEPT ![a] = IF kind = "barrier" /\ result[<<a, kind>>] = "ok"
        THEN (IF Focus = "external" THEN "external" ELSE "read") ELSE "done"]
    /\ held' = IF kind = "barrier" /\ result[<<a, kind>>] = "ok" THEN held ELSE held \ {a}
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, context, staleAtEntry, tableRead, partRead, schemaRead,
                    rootRead, versionRead, order, requestSession, processed, result, failed, lateWrite, staleExternal>>
External(a) ==
    /\ pc[a] = "external" /\ pc' = [pc EXCEPT ![a] = "read"]
    /\ staleExternal' = (staleExternal \/ staleAtEntry[a])
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, held, context, staleAtEntry, tableRead, partRead, schemaRead,
                    rootRead, versionRead, order, requestSession, processed, result, failed, lateWrite>>
Readable(a) == IF a \in Creates THEN (IF Target = "table" THEN ~live ELSE live /\ ~partLive[Partition])
               ELSE live /\ (Target = "table" \/ partLive[Partition])
InputMatches(a) == a # "d" \/ ~CachedInput \/ ~VerifyInput
                   \/ tableId = 0
Read(a) ==
    /\ pc[a] = "read" /\ SessionValid(a) /\ ReadsOrdered(a)
    /\ pc' = [pc EXCEPT ![a] = IF Readable(a) /\ InputMatches(a) THEN "send" ELSE "done"]
    /\ held' = IF Readable(a) /\ InputMatches(a) THEN held ELSE held \ {a}
    /\ tableRead' = [tableRead EXCEPT ![a] = IF a = "d" /\ CachedInput THEN 0 ELSE tableId]
    /\ partRead' = [partRead EXCEPT ![a] = partId[Partition]]
    /\ schemaRead' = [schemaRead EXCEPT ![a] = schema]
    /\ rootRead' = [rootRead EXCEPT ![a] = rootVersion]
    /\ versionRead' = [versionRead EXCEPT ![a] = partVersion[Partition]]
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, context, staleAtEntry, order, requestSession, processed, result, failed, lateWrite, staleExternal>>
Error(a) ==
    /\ (pc[a] \in {"barrier-wait", "wait"} \/ (pc[a] \in {"barrier-send", "read", "send"} /\ ~SessionValid(a)))
    /\ pc' = [pc EXCEPT ![a] = "done"] /\ held' = held \ {a} /\ failed' = failed \cup {a}
    /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion, history,
                    epoch, replaced, context, staleAtEntry, tableRead, partRead, schemaRead,
                    rootRead, versionRead, order, requestSession, processed, result, lateWrite, staleExternal>>
Valid(r) ==
    /\ (context[r[1]] = epoch \/ Mode = "no-epoch")
    /\ (IF r[2] = "barrier" THEN TRUE
         ELSE CASE Op(r[1]) = "create-table" -> ~live
              [] Op(r[1]) = "create-part" -> live /\ ~partLive[Partition]
              [] Op(r[1]) = "drop-table" -> live /\ rootVersion = rootRead[r[1]]
              [] Op(r[1]) = "alter-table" -> live /\ schema = schemaRead[r[1]]
              [] OTHER -> live /\ partLive[Partition] /\ partVersion[Partition] = versionRead[r[1]])
C == INSTANCE DdlMetadataContract WITH Parts <- Parts, live <- live, partLive <- partLive,
     tableId <- tableId, partId <- partId, schema <- schema, history <- history
Apply(r) ==
    /\ r \in Sent \ processed /\ Before(r) /\ processed' = processed \cup {r}
    /\ result' = [result EXCEPT ![r] = IF Valid(r) THEN "ok" ELSE "error"]
    /\ LET a == r[1] op == Op(a) publish == Valid(r) /\ r[2] = "mutation" IN
       /\ live' = IF publish /\ op = "drop-table" THEN FALSE ELSE IF publish /\ op = "create-table" THEN TRUE ELSE live
       /\ partLive' = IF publish /\ op = "drop-table" THEN [p \in Parts |-> FALSE]
                       ELSE IF publish /\ op = "create-table" THEN [p \in Parts |-> TRUE]
                       ELSE IF publish /\ op = "drop-part" THEN [partLive EXCEPT ![Partition] = FALSE]
                       ELSE IF publish /\ op = "create-part" THEN [partLive EXCEPT ![Partition] = TRUE] ELSE partLive
       /\ tableId' = IF publish /\ op = "create-table" THEN tableId + 1 ELSE tableId
       /\ partId' = IF publish /\ op = "create-table" THEN [p \in Parts |-> partId[p] + 1]
                     ELSE IF publish /\ op = "create-part" THEN [partId EXCEPT ![Partition] = @ + 1] ELSE partId
       /\ schema' = IF publish /\ op = "create-table" THEN 0 ELSE IF publish /\ op = "alter-table" THEN schema + 1 ELSE schema
       /\ rootVersion' = IF publish /\ op = "create-table" THEN 1 ELSE rootVersion
       /\ partVersion' = IF publish /\ op = "create-table" THEN [p \in Parts |-> 0]
                          ELSE IF publish /\ op = "create-part" THEN [partVersion EXCEPT ![Partition] = 0]
                          ELSE IF publish /\ op = "alter-part" THEN [partVersion EXCEPT ![Partition] = @ + 1] ELSE partVersion
       /\ history' = IF publish THEN Append(history, C!Record(op, tableRead[a], partRead[a], Partition)) ELSE history
       /\ lateWrite' = (lateWrite \/ (publish /\ a \in failed))
    /\ UNCHANGED <<epoch, replaced, pc, held, context, staleAtEntry, tableRead, partRead,
                    schemaRead, rootRead, versionRead, order, requestSession, failed, staleExternal>>
Failover == /\ AllowFailover /\ epoch = 0 /\ epoch' = 1
            /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion,
                history, replaced, pc, held, context, staleAtEntry, tableRead, partRead, schemaRead,
                rootRead, versionRead, order, requestSession, processed, result, failed, lateWrite, staleExternal>>
Reconnect == /\ AllowReconnect /\ ~replaced /\ replaced' = TRUE
             /\ UNCHANGED <<live, partLive, tableId, partId, schema, rootVersion, partVersion,
                history, epoch, pc, held, context, staleAtEntry, tableRead, partRead, schemaRead,
                rootRead, versionRead, order, requestSession, processed, result, failed, lateWrite, staleExternal>>
Step(a) == Acquire(a) \/ Start(a) \/ Send(a, "barrier") \/ Reply(a, "barrier")
           \/ External(a) \/ Read(a) \/ Send(a, "mutation") \/ Reply(a, "mutation") \/ Error(a)
Finished == (\A a \in Actors : pc[a] = "done") /\ Sent \subseteq processed
Next == Failover \/ Reconnect \/ (\E a \in Actors : Step(a)) \/ (\E r \in Requests : Apply(r)) \/ (Finished /\ UNCHANGED vars)
Spec == Init /\ [][Next]_vars /\ (\A a \in Actors : WF_vars(Step(a)))
        /\ (\A r \in Requests : WF_vars(Apply(r)))
        /\ (IF Focus = "external" THEN WF_vars(Failover) ELSE TRUE)
Refines == C!Spec
NoWrongIncarnation == C!NoWrongIncarnation
Resources == \A a \in Actors : pc[a] = "done" => a \notin held
Progress == <>Finished
NoKnownStaleExternal == ~staleExternal
NeverTwoRecreations == ~(tableId = 2 \/ partId[Partition] = 2)
NeverLateWrite == ~lateWrite
=============================================================================
