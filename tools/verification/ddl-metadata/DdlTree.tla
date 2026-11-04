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
------------------------------- MODULE DdlTree -------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Tables, Parts, Strategy, Events, AllowUnknown, AllowFailover,
          CheckEpoch, WaitForKnown, MaintainCounts, MissingAssignment, GuardUnknown
VARIABLES published, schemaAvailable, roots, childNodes, rootVersion, epoch,
          pc, held, done, target, request, requestChild, snapshotVersion, pending,
          replied, ok, withdrawn, fullDeleted, notified, parentQueued, childQueued,
          parentInfo, known, assignments, count, countEntries, startupAttempted,
          startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite
vars == <<published, schemaAvailable, roots, childNodes, rootVersion, epoch,
          pc, held, done, target, request, requestChild, snapshotVersion, pending,
          replied, ok, withdrawn, fullDeleted, notified, parentQueued, childQueued,
          parentInfo, known, assignments, count, countEntries, startupAttempted,
          startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Children == Tables \X Parts
ForTable(children, t) == {c \in children : c[1] = t}
Runtime == runtimeEpoch = epoch
Current == CHOOSE t \in Tables \ done : TRUE
Kind(t) == IF Strategy = "withdraw" /\ t \in published THEN "withdraw"
           ELSE IF t \in schemaAvailable THEN "schema"
           ELSE IF ForTable(childNodes, t) # {} THEN "child" ELSE "root"
Init == /\ published = Tables /\ schemaAvailable = Tables /\ roots = Tables
        /\ childNodes = Children /\ rootVersion = [t \in Tables |-> 1] /\ epoch = 0
        /\ pc = "ready" /\ held = TRUE /\ done = {} /\ target = CHOOSE t \in Tables : TRUE
        /\ request = "none" /\ requestChild = CHOOSE c \in Children : TRUE
        /\ snapshotVersion = 0 /\ pending = FALSE /\ replied = FALSE /\ ok = FALSE
        /\ withdrawn = {} /\ fullDeleted = {} /\ notified = {} /\ parentQueued = {} /\ childQueued = {}
        /\ parentInfo = Tables /\ known = Children
        /\ assignments = IF MissingAssignment THEN Children \ {CHOOSE c \in Children : TRUE} ELSE Children
        /\ count = [t \in Tables |-> Cardinality(ForTable(Children, t))] /\ countEntries = Tables
        /\ startupAttempted = FALSE /\ startupFailed = FALSE /\ runtimeEpoch = 0
        /\ orphanObserved = {} /\ eventFailed = FALSE /\ oldEpochWrite = FALSE
Send == /\ pc = "ready" /\ done # Tables /\ Current \in roots
        /\ target' = Current /\ request' = Kind(Current)
        /\ requestChild' = IF ForTable(childNodes, Current) = {} THEN requestChild ELSE CHOOSE c \in ForTable(childNodes, Current) : TRUE
        /\ snapshotVersion' = rootVersion[Current]
        /\ pending' = TRUE /\ replied' = FALSE /\ pc' = "wait"
        /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, held, done, ok,
                        withdrawn, fullDeleted, notified, parentQueued, childQueued, parentInfo, known, assignments,
                        count, countEntries, startupAttempted, startupFailed, runtimeEpoch, orphanObserved,
                        eventFailed, oldEpochWrite>>
Valid == /\ (epoch = 0 \/ ~CheckEpoch) /\ target \in roots
         /\ CASE request = "withdraw" -> target \in published /\ rootVersion[target] = snapshotVersion
              [] request = "schema" -> target \in schemaAvailable
              [] request = "child" -> requestChild \in childNodes
              [] request = "root" -> target \notin schemaAvailable /\ ForTable(childNodes, target) = {} /\ rootVersion[target] = snapshotVersion
              [] OTHER -> FALSE
Apply == /\ pending /\ pending' = FALSE /\ replied' = TRUE /\ ok' = Valid
         /\ published' = IF Valid /\ request \in {"withdraw", "root"} THEN published \ {target} ELSE published
         /\ schemaAvailable' = IF Valid /\ request \in {"schema"} THEN schemaAvailable \ {target} ELSE schemaAvailable
         /\ roots' = IF Valid /\ request \in {"root"} THEN roots \ {target} ELSE roots
         /\ childNodes' = IF Valid /\ request = "child" THEN childNodes \ {requestChild}
                          ELSE childNodes
         /\ rootVersion' = IF Valid /\ request = "withdraw" THEN [rootVersion EXCEPT ![target] = @ + 1] ELSE rootVersion
         /\ withdrawn' = IF Valid /\ request = "withdraw" THEN withdrawn \cup {target} ELSE withdrawn
         /\ fullDeleted' = IF Valid /\ request \in {"root"} /\ target \in published THEN fullDeleted \cup {target} ELSE fullDeleted
         /\ oldEpochWrite' = (oldEpochWrite \/ (Valid /\ epoch # 0))
         /\ UNCHANGED <<epoch, pc, held, done, target, request, requestChild, snapshotVersion,
                         notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                         startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed>>
Reply == /\ pc = "wait" /\ replied /\ pc' = IF ok THEN "ready" ELSE "done"
         /\ held' = IF ok THEN held ELSE FALSE
         /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, done, target,
                         request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                         notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                         startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Advance == /\ pc = "ready" /\ done # Tables /\ Current \notin roots /\ done' = done \cup {Current}
           /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, target,
                           request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                           notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                           startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Finish == /\ pc = "ready" /\ done = Tables /\ pc' = "done" /\ held' = FALSE
          /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, done, target,
                          request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                          notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                          startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Unknown == /\ AllowUnknown /\ pc = "wait" /\ pc' = "done" /\ held' = FALSE
           /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, done, target,
                           request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                           notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                           startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Failover == /\ AllowFailover /\ epoch = 0 /\ epoch' = 1
            /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, pc, held, done, target,
                            request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                            notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                            startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Recover == /\ epoch = 1 /\ ~startupAttempted /\ startupAttempted' = TRUE
           /\ startupFailed' = ~(published \subseteq schemaAvailable)
           /\ parentInfo' = IF published \subseteq schemaAvailable THEN published ELSE parentInfo
           /\ known' = IF published \subseteq schemaAvailable THEN {c \in childNodes : c[1] \in published} ELSE known
           /\ count' = [t \in Tables |-> Cardinality(ForTable(known', t))]
           /\ countEntries' = {t \in Tables : count'[t] > 0} /\ runtimeEpoch' = 1
           /\ parentQueued' = {} /\ childQueued' = {} /\ notified' = {}
           /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done, target,
                           request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted, assignments,
                           orphanObserved, eventFailed, oldEpochWrite>>
NotifyParent(t) == /\ Events /\ Runtime /\ t \in parentInfo /\ t \notin notified
                   /\ t \in (withdrawn \cup fullDeleted)
                   /\ notified' = notified \cup {t} /\ parentQueued' = parentQueued \cup {t}
                   /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                       target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                       childQueued, parentInfo, known, assignments, count, countEntries, startupAttempted, startupFailed,
                       runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
NotifyChild(c) == /\ Events /\ Runtime /\ c \in known /\ c \notin childNodes /\ c \notin childQueued
                  /\ childQueued' = childQueued \cup {c}
                  /\ eventFailed' = (eventFailed \/ c[1] \notin parentInfo)
                  /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                      target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                      notified, parentQueued, parentInfo, known, assignments, count, countEntries, startupAttempted,
                      startupFailed, runtimeEpoch, orphanObserved, oldEpochWrite>>
RetireChild(c) == /\ Events /\ Runtime /\ c \in childQueued /\ c \in known
                  /\ known' = known \ {c} /\ assignments' = assignments \ {c} /\ childQueued' = childQueued \ {c}
                  /\ count' = IF MaintainCounts THEN [count EXCEPT ![c[1]] = @ - 1] ELSE count
                  /\ countEntries' = {t \in countEntries : count'[t] > 0}
                  /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                      target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                      notified, parentQueued, parentInfo, startupAttempted, startupFailed, runtimeEpoch, orphanObserved,
                      eventFailed, oldEpochWrite>>
RetireParent(t) == /\ Events /\ Runtime /\ t \in parentQueued /\ t \in parentInfo
                   /\ ForTable(childQueued, t) = {} /\ (~WaitForKnown \/ t \notin countEntries)
                   /\ parentInfo' = parentInfo \ {t} /\ parentQueued' = parentQueued \ {t}
                   /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                       target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                       notified, childQueued, known, assignments, count, countEntries, startupAttempted, startupFailed,
                       runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
ObserveOrphan(c) == /\ Events /\ Runtime /\ startupAttempted /\ ~startupFailed
                    /\ c \in childNodes /\ c \notin known /\ c \notin orphanObserved
                    /\ orphanObserved' = orphanObserved \cup {c}
                    /\ eventFailed' = (eventFailed \/ ~GuardUnknown)
                    /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                        target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                        notified, parentQueued, childQueued, parentInfo, known, assignments, count, countEntries,
                        startupAttempted, startupFailed, runtimeEpoch, oldEpochWrite>>
CleanupOrphan(c) == /\ Events /\ Runtime /\ startupAttempted /\ ~startupFailed
                    /\ c \in assignments /\ c[1] \notin published /\ assignments' = assignments \ {c}
                    /\ UNCHANGED <<published, schemaAvailable, roots, childNodes, rootVersion, epoch, pc, held, done,
                        target, request, requestChild, snapshotVersion, pending, replied, ok, withdrawn, fullDeleted,
                        notified, parentQueued, childQueued, parentInfo, known, count, countEntries,
                        startupAttempted, startupFailed, runtimeEpoch, orphanObserved, eventFailed, oldEpochWrite>>
Worker == Send \/ Reply \/ Advance \/ Finish \/ Unknown
Callbacks == (\E t \in Tables : NotifyParent(t) \/ RetireParent(t))
             \/ (\E c \in Children : NotifyChild(c) \/ RetireChild(c) \/ ObserveOrphan(c) \/ CleanupOrphan(c))
Next == Worker \/ Apply \/ Failover \/ Recover \/ Callbacks \/ (pc = "done" /\ ~pending /\ UNCHANGED vars)
Spec == Init /\ [][Next]_vars /\ WF_vars(Worker) /\ WF_vars(Apply) /\ WF_vars(Recover)
        /\ (\A t \in Tables : WF_vars(NotifyParent(t)) /\ WF_vars(RetireParent(t)))
        /\ (\A c \in Children : WF_vars(NotifyChild(c)) /\ WF_vars(RetireChild(c)) /\ WF_vars(CleanupOrphan(c)))
C == INSTANCE DdlTreeContract WITH Tables <- Tables, Parts <- Parts,
     published <- published, schemaAvailable <- schemaAvailable, roots <- roots,
     parentInfo <- parentInfo, known <- known
Refines == C!Spec
PublishedHasSchema == C!PublishedHasSchema
ParentWhileKnown == C!ParentWhileKnown
CountsExact == \A t \in Tables : count[t] = Cardinality(ForTable(known, t))
CountEntriesExact == countEntries = {t \in Tables : count[t] > 0}
CountEntryBound == Cardinality(countEntries) <= Cardinality(known)
NoStartupFailure == ~startupFailed
NoEventFailure == ~eventFailed
NoOldEpochWrite == ~oldEpochWrite
Resources == pc = "done" => ~held
Progress == <> (pc = "done" /\ ~pending)
RetirementProgress == <>[] (parentInfo = {} /\ known = {} /\ countEntries = {})
RecoveryProgress == (epoch = 1) ~> startupAttempted
OrphanProgress == \A c \in Children : (startupAttempted /\ ~startupFailed /\ c[1] \notin published) ~> (c \notin assignments)
NeverIncompleteWithdraw == ~(pc = "done" /\ epoch = 0 /\ withdrawn # {} /\ childNodes # {})
NeverRecoveredWithdraw == ~(startupAttempted /\ ~startupFailed /\ withdrawn # {} /\ roots # {})
=============================================================================
