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
---------------------------- MODULE DdlTreeContract ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Tables, Parts
VARIABLES published, schemaAvailable, roots, parentInfo, known
vars == <<published, schemaAvailable, roots, parentInfo, known>>
Children == Tables \X Parts
Init == /\ published = Tables /\ schemaAvailable = Tables /\ roots = Tables
        /\ parentInfo = Tables /\ known = Children
Withdraw(t) == /\ t \in published /\ published' = published \ {t}
               /\ UNCHANGED <<schemaAvailable, roots, parentInfo, known>>
DeleteSchema(t) == /\ t \notin published /\ schemaAvailable' = schemaAvailable \ {t}
                   /\ UNCHANGED <<published, roots, parentInfo, known>>
DeleteRoot(t) == /\ t \notin published /\ t \notin schemaAvailable /\ roots' = roots \ {t}
                 /\ UNCHANGED <<published, schemaAvailable, parentInfo, known>>
RetireChild(c) == /\ c \in known /\ c[1] \in parentInfo /\ known' = known \ {c}
                  /\ UNCHANGED <<published, schemaAvailable, roots, parentInfo>>
RetireParent(t) == /\ {c \in known : c[1] = t} = {} /\ parentInfo' = parentInfo \ {t}
                   /\ UNCHANGED <<published, schemaAvailable, roots, known>>
Recover == /\ parentInfo' = published
           /\ known' \subseteq {c \in Children : c[1] \in published}
           /\ UNCHANGED <<published, schemaAvailable, roots>>
Next == Recover \/ (\E t \in Tables : Withdraw(t) \/ DeleteSchema(t) \/ DeleteRoot(t) \/ RetireParent(t))
        \/ (\E c \in Children : RetireChild(c))
Spec == Init /\ [][Next]_vars
PublishedHasSchema == published \subseteq schemaAvailable
ParentWhileKnown == \A c \in known : c[1] \in parentInfo
=============================================================================
