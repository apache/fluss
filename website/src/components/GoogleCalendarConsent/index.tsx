/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, {useState} from 'react';

export default function GoogleCalendarConsent({calendarId}) {
    const [loaded, setLoaded] = useState(false);

    const loadCalendar = () => {
        setLoaded(true);
    };

    return (
        <div className="calendar-consent-container">
            {!loaded ? (
                <div
                    onClick={loadCalendar}
                    style={{
                        textAlign: "center",
                        width: '800px',
                        height: '600px',
                        cursor: 'pointer',
                        backgroundImage: `url("/img/placeholder_calendar.png")`,
                        backgroundSize: 'cover',
                    }}
                >
                </div>
            ) : (
                <div className="calendar-iframe-wrapper">
                    <iframe
                        width="800"
                        height="600"
                        src={`https://calendar.google.com/calendar/embed?src=${calendarId}%40group.calendar.google.com&ctz=Greenwich`}
                        frameBorder="0"
                        scrolling="no"
                    />
                </div>
            )}
        </div>
    );
}
