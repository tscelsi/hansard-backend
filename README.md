# augov projects

A repo of projects surfacing legislation and clarifying the bills passed in the houses of government in australia

1. keep track of all acts "In force"
2. visual of an act over time. The changes shown, like git reviews, and some historical of who was in power etc.



Holding representatives accountable?

## API to query member's speeches in parliament

GET /members - list members
GET /members/{member_id}/speeches

GET /bills - list bills discussed in parliament
GET /bills/{bill_id}/speeches

## other stuff

Simple aggregations:

- How many times has X interjected?
- \# speeches given by party
- list of most frequent speakers

From the perspective of a parliament member:
- How often does X speak?
  - avg. speeches per parliamentary session
- What sorts of bills and occasions is X speaking?
- What are X's latest speech exerpts? for what bills?
- how many speeches & interjections
  - this year, this government, all time

From the perspective of a bill:
- what house is the bill currently in?
- what stage is the bill at?
- how many speeches have been made on this bill?
- divisiveness of the bill (based on number of interjections? and sentiment)
- which members are for/against the bill (based on sentiment analysis of speeches)
- which parties are for/against the bill (based on sentiment analysis of speeches)
- What parties are speaking on this bill?
- What parliament members are speaking on this bill?
- for a bill, lets see all the debate, who have been the main players?


More complex questions:
- stances and framing
- What has X been saying in their speeches?
- What is X's stance on the things they are talking about?

class Speech:
    speech_id: UUID
    speech_parts: list[SpeechPart]
    main_talker_id: str
    debate_category: str # e.g. "BILLS" | "STATEMENTS BY MEMBERS"
    bill_id: str | None  # only for "BILLS"
    debate_title: str
    debate_info: str
    subdebate_title: str | None
    subdebate_info: str | None


A speech in parliament is made up of speech parts. A speech part is a continuous block of text spoken by a single talker. When a talker gives a speech, they may be interrupted by interjections from otheer talkers. Each interjection is its own speech part and the sequence of speech parts together make up the full speech, including the interjections.

The goal of this repository is to make more available and accessible the data surrounding parliamentary speeches in Australia, enabling analysis and insights into parliamentary proceedings. It aims to provide a way to query speeches given by members of parliament, the bills discussed, and various aggregations and statistics related to parliamentary debates.



## TODO


1. parse time out of first speech part and assign to speech.date
2. parse out deputy speaker tag

### NLP things
1. topic modelling on speech contents and labelling of topics to act as tags
   1. mmmm not very compelling once i've tried. The topics are very vague.
2. fact check divisive statements in speeches
3. classify attacks against opposing parties/members and understand if they are personal or policy based
4. check for hypocrisy? pretty hard
5. find similar/opposite statements in different speeches (by the same speaker or not etc.)
6. Similar speech clustering with embeddings
7. what constituencies are begin spoken about by different speakers? Who do they represent?
8. compare speeches on similar bills to see how different parties frame the same issue



A day after parliament sitting:

1. create schedule to run after sitting days (or just run it every day at midnight and it will do nothing if no new hansard)
2. if any new hansard xml files:
    1. download hansard xml
    2. parse hansard xml into speeches and speech parts
    3. update talker list
    4. save speeches, speech parts to db
    5. run nlp tasks on new speeches (summarisation, sentiment analysis, divisiveness etc.)
