#[derive(Clone, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub request: String,
}

#[derive(Clone, Debug)]
pub struct MakeRequest {
    pub request: String,
}

#[derive(Clone, Debug)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug)]
pub struct Vote {
    pub term: u64,
    pub voter_id: u64,
    pub granted: bool,
}

#[derive(Clone, Debug)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug)]
pub struct AppendEntriesResponse {
    pub node_id: u64,
    pub term: u64,
    pub prev_log_index: u64,
    pub success: bool,
}

#[derive(Debug)]
pub enum Outbound {
    MakeRequest(MakeRequest),
    RequestVote(RequestVote),
    Vote(Vote),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl Into<Outbound> for MakeRequest {
    fn into(self) -> Outbound {
        Outbound::MakeRequest(self)
    }
}

impl Into<Outbound> for RequestVote {
    fn into(self) -> Outbound {
        Outbound::RequestVote(self)
    }
}

impl Into<Outbound> for Vote {
    fn into(self) -> Outbound {
        Outbound::Vote(self)
    }
}

impl Into<Outbound> for AppendEntries {
    fn into(self) -> Outbound {
        Outbound::AppendEntries(self)
    }
}

impl Into<Outbound> for AppendEntriesResponse {
    fn into(self) -> Outbound {
        Outbound::AppendEntriesResponse(self)
    }
}

pub trait Recv<Event> {
    fn recv(&self, event: Event);
}
