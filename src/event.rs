#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub request: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MakeRequest {
    pub request: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NetworkUpdateInd {
    pub leader_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateUpdateRequest {
    pub included_index: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateUpdateResponse {
    pub node_id: u64,
    pub included_index: u64,
    pub included_term: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StateUpdateCommand {
    pub node_id: u64,
    pub included_index: u64,
    pub included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestPreVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreVote {
    pub term: u64,
    pub voter_id: u64,
    pub granted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Vote {
    pub term: u64,
    pub voter_id: u64,
    pub granted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendEntriesResponse {
    pub node_id: u64,
    pub term: u64,
    pub prev_log_index: u64,
    pub success: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstallSnapshot {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstallSnapshotResponse {
    pub node_id: u64,
    pub term: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommitNotification {
    pub node_id: u64,
    pub index: u64,
    pub term: u64,
    pub request: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum Protocol {
    RequestPreVote(RequestPreVote),
    PreVote(PreVote),
    RequestVote(RequestVote),
    Vote(Vote),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

#[derive(Debug, PartialEq)]
pub enum Outbound {
    NetworkUpdateInd(NetworkUpdateInd),
    CommitNotification(CommitNotification),
    StateUpdateResponse(StateUpdateResponse),
    StateUpdateCommand(StateUpdateCommand),
    MessageToPeer(u64, Protocol),
}

impl From<NetworkUpdateInd> for Outbound {
    fn from(val: NetworkUpdateInd) -> Self {
        Outbound::NetworkUpdateInd(val)
    }
}

impl From<CommitNotification> for Outbound {
    fn from(val: CommitNotification) -> Self {
        Outbound::CommitNotification(val)
    }
}

impl From<StateUpdateResponse> for Outbound {
    fn from(val: StateUpdateResponse) -> Self {
        Outbound::StateUpdateResponse(val)
    }
}

impl From<StateUpdateCommand> for Outbound {
    fn from(val: StateUpdateCommand) -> Self {
        Outbound::StateUpdateCommand(val)
    }
}

impl From<RequestPreVote> for Protocol {
    fn from(val: RequestPreVote) -> Self {
        Protocol::RequestPreVote(val)
    }
}

impl From<PreVote> for Protocol {
    fn from(val: PreVote) -> Self {
        Protocol::PreVote(val)
    }
}

impl From<RequestVote> for Protocol {
    fn from(val: RequestVote) -> Self {
        Protocol::RequestVote(val)
    }
}

impl From<Vote> for Protocol {
    fn from(val: Vote) -> Self {
        Protocol::Vote(val)
    }
}

impl From<AppendEntries> for Protocol {
    fn from(val: AppendEntries) -> Self {
        Protocol::AppendEntries(val)
    }
}

impl From<AppendEntriesResponse> for Protocol {
    fn from(val: AppendEntriesResponse) -> Self {
        Protocol::AppendEntriesResponse(val)
    }
}

impl From<InstallSnapshot> for Protocol {
    fn from(val: InstallSnapshot) -> Self {
        Protocol::InstallSnapshot(val)
    }
}

impl From<InstallSnapshotResponse> for Protocol {
    fn from(val: InstallSnapshotResponse) -> Self {
        Protocol::InstallSnapshotResponse(val)
    }
}

pub trait Recv<Event> {
    fn recv(&self, event: Event);
}
