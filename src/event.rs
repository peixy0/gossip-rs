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
}

#[derive(Clone, Debug)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug)]
pub enum Outbound {
    RequestVote(RequestVote),
    Vote(u64, Vote),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(u64, AppendEntriesResponse),
}

pub trait Recv<Event> {
    fn recv(&self, event: Event);
}
