---
title: Hosting Community Calls
sidebar_position: 5
---

# Hosting Community Calls

Fluss hosts monthly community calls on the **last Friday of each month** to accommodate contributors across the world time zones. These calls are an opportunity for the community to connect, share updates, discuss roadmap items, and collaborate on the project's future. Check the [Community Events Calendar](https://calendar.google.com/calendar/u/0?cid=Y19mOGNmOWMzZGM0MTlmYjllOGJiNGMxNjQ4NzViMmNmZjQyMmE2NzUxMGVhMzc4OGY5ZmIzODE5YzY2N2ZjNmQxQGdyb3VwLmNhbGVuZGFyLmdvb2dsZS5jb20) for upcoming call schedules.

Any community member can volunteer to host these calls. This guide provides step-by-step instructions for hosting and documenting community calls using [Fathom.ai](https://www.fathom.ai/) for automated recording and note-taking.

:::tip
All community workflows in Fluss are transparent and documented, similar to our [release process](/community/how-to-release/release-manager-preparation). Anyone can take on the role of call host by following this guide.
:::

## Prerequisites

- Personal Gmail account (recommended over work email to avoid restrictions)
- Access to Fluss community Google Meet link
- 10 minutes for Fathom.ai setup

## Announce the Community Call

A week or a day before the call, post this message in [#general Slack channel](https://apache-fluss.slack.com/archives/C07FCUA8SKS):

```
Hi everyone 👋

We're excited to invite you to our upcoming Fluss community call on Friday, [DATE].
Please feel free to join and bring along any topics or agenda items you'd like to discuss.

In this community call, we'll be discussing [TOPICS - e.g., Fluss 0.9 release, key takeaways,
and what's ahead as we move toward 1.0].

It's a great opportunity to share your feedback, bring in real-world production insights,
and help shape the priorities that matter most to the community.

Looking forward to the discussion and hearing your thoughts!

Join using: [Google Meet Link]
📅 Time Zones:
- Europe (CET): 9:00 – 9:45 AM
- India (IST): 1:30 – 2:15 PM
- China (CST): 4:00 – 4:45 PM
- US (PDT): 1:00 – 1:45 AM

📝 Add discussion points here: [Community Sync Google Doc]
```

## Set Up Google Doc for Agenda

Before the call, update the [Community Sync Google Doc](https://docs.google.com/document/d/DOCUMENT_ID) with:

```
## Community Call - [DATE]


### Discussion Topics

#### Community Member Topics
- [Members add what they want to discuss]
-

### Meeting Notes
[Will be added after call from Fathom recording]


### Recording
🎥 [Fathom link - will be added after call]
```

:::note
The Google Doc is **mandatory** - it allows participants to add discussion points before the call and serves as a permanent record.
:::

## Setting Up Fathom.ai

[Fathom.ai](https://www.fathom.ai/) is a free AI-powered meeting assistant that records, transcribes, and summarizes Google Meet calls.

### Account Creation & Setup

1. Visit [https://www.fathom.ai/](https://www.fathom.ai/) and sign up with your **personal Gmail account**
2. Download and install the [Fathom desktop app](https://www.fathom.ai/download)
3. In the Fathom dashboard, go to **Settings** → **Integrations**
4. Connect your **Google Calendar** and **Google Meet**
5. Enable **Auto-record** in Settings → Preferences

:::warning Personal Email Sign-Up
When signing up with a personal email (gmail.com, yahoo.com), Fathom requires you have **at least one meeting scheduled in the next 7 days**. If you don't have a meeting scheduled, Fathom may block your account creation.

**Solution**: Make sure the Fluss community call is scheduled in your Google Calendar before signing up.
:::

## During the Community Call

1. Open the Fathom desktop app
2. Join the Google Meet call
3. Fathom's notetaker bot will appear in the **waiting room** - admit it to the meeting
4. From the Fathom desktop app, click **Record** to start recording
5. Announce to participants that the call is being recorded

:::note
Only the **meeting host** can admit the Fathom notetaker from the waiting room.
:::

## After the Call

Fathom will automatically generate:
- Full transcript
- AI summary
- Key highlights and action items

## Sharing Meeting Notes

### 1. Update Google Doc (Mandatory)

1. Go to the [Fathom dashboard](https://app.fathom.video/) and find your recording
2. Copy the Fathom recording link (click **Share** → set to "Anyone with the link")
3. Update the [Community Sync Google Doc](https://docs.google.com/document/d/DOCUMENT_ID):
   - Copy and refine the AI-generated summary from Fathom
   - Add action items with owners
   - Paste the Fathom recording link

### 2. Post to Slack

Post in the [#general channel](https://apache-fluss.slack.com/archives/C07FCUA8SKS):

```
📞 Community Call - [Month Day, Year] Summary

Thanks everyone who joined! Here's a quick recap:
- [Key point 1]
- [Key point 2]
- [Key point 3]

🎥 Recording: [Fathom Link]
📝 Community sync Notes: [Google Doc Link]

Next call: [Date]
```

### 3. Email to Mailing List

Send to dev@fluss.apache.org with subject: `[Community Call] Summary - [Month Year]`

Include:
- Brief summary
- Recording link
- Google Doc link
- Action items

## Questions?

For questions about hosting community calls, reach out on:
- [Slack #general](https://apache-fluss.slack.com/archives/C07FCUA8SKS)
- Mailing list: dev@fluss.apache.org

Thank you for volunteering to host community calls!