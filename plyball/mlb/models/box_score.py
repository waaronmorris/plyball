from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class SpringLeague(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    link: Optional[str] = None
    abbreviation:  Optional[str] = None


class Venue(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    link: Optional[str] = None


class SpringVenue(BaseModel):
    id: Optional[int] = None
    link: Optional[str] = None


class League(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    link: Optional[str] = None


class Division(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    link: Optional[str] = None


class Sport(BaseModel):
    id: int
    link: str
    name: str


class LeagueRecord(BaseModel):
    wins: int
    losses: int
    ties: int
    pct: str


class Record(BaseModel):
    gamesPlayed: int
    wildCardGamesBack: str
    leagueGamesBack: str
    springLeagueGamesBack: Optional[str] = None
    sportGamesBack: Optional[str] = None
    divisionGamesBack: str
    conferenceGamesBack: str
    leagueRecord: LeagueRecord
    records: Dict[str, Any]
    divisionLeader: bool
    wins: int
    losses: int
    winningPercentage: str


class TeamData(BaseModel):
    springLeague: Optional[SpringLeague] = None
    allStarStatus: str
    id: int
    name: str
    link: str
    season: int
    venue: Venue
    springVenue: Optional[SpringVenue] = None
    teamCode: str
    fileCode: str
    abbreviation: str
    teamName: str
    locationName: str
    firstYearOfPlay: str
    league: League
    division: Optional[Division] = None
    sport: Sport
    shortName: str
    record: Record
    franchiseName: str
    clubName: str
    active: bool


class Person(BaseModel):
    id: int
    fullName: str
    link: str


class PlayerPosition(BaseModel):
    code: str
    name: str
    type: str
    abbreviation: str


class PlayerStatus(BaseModel):
    code: str
    description: str


class PlayerGameStats(BaseModel):
    batting: Dict[str, Any]
    pitching: Dict[str, Any]
    fielding: Dict[str, Any]


class PlayerSeasonStats(BaseModel):
    batting: Dict[str, Any]
    pitching: Dict[str, Any]
    fielding: Dict[str, Any]


class PlayerGameStatus(BaseModel):
    isCurrentBatter: bool
    isCurrentPitcher: bool
    isOnBench: bool
    isSubstitute: bool


class Player(BaseModel):
    person: Person
    jerseyNumber: Optional[str] = None
    position: Optional[PlayerPosition] = None
    status: Optional[PlayerStatus] = None
    parentTeamId: Optional[int] = None
    battingOrder: Optional[str] = None
    stats: Optional[PlayerGameStats] = None
    seasonStats: Optional[PlayerSeasonStats] = None
    gameStatus: Optional[PlayerGameStatus] = None
    allPositions: Optional[List[AllPosition]] =   None


class AllPosition(BaseModel):
    code: str
    name: str
    type: str
    abbreviation: str


class FieldListItem(BaseModel):
    label: str
    value: str


class TeamInfoItem(BaseModel):
    title: str
    fieldList: List[FieldListItem]


class TeamNoteItem(BaseModel):
    label: str
    value: str


class Team(BaseModel):
    team: TeamData
    teamStats: TeamStats
    players: Dict[str, Player]
    batters: List[int]
    pitchers: List[int]
    bench: List[int]
    bullpen: List[int]
    battingOrder: List[int]
    info: List[TeamInfoItem]
    note: List[TeamNoteItem]


class TeamStats(BaseModel):
    batting: Optional[Dict[str, Any]]
    pitching: Optional[Dict[str, Any]]
    fielding: Optional[Dict[str, Any]]


class Teams(BaseModel):
    away: Team
    home: Team


class OfficialData(BaseModel):
    id: int
    fullName: str
    link: str


class Official(BaseModel):
    official: OfficialData
    officialType: str


class BoxScoreInfo(BaseModel):
    label: Optional[str] = None
    value: Optional[str] = None


class TopPerformingPlayer(BaseModel):
    person: Person
    jerseyNumber: Optional[str] = None
    position: PlayerPosition
    status: PlayerStatus
    parentTeamId: Optional[int] = None
    battingOrder: Optional[str] = None
    stats: PlayerGameStats
    seasonStats: PlayerSeasonStats
    gameStatus: PlayerGameStatus
    allPositions: Optional[List[AllPosition]] = None


class TopPerformer(BaseModel):
    player: Optional[TopPerformingPlayer] = None
    type: Optional[str] = None
    gameScore: Optional[int] = None
    hittingGameScore: Optional[int] = None
    pitchingGameScore: Optional[int] = None


class BoxScoreResponse(BaseModel):
    copyright: Optional[str] = None
    teams: Teams
    officials: List[Official]
    info: List[BoxScoreInfo]
    pitchingNotes: List[Union[str,None]] = None
    topPerformers: List[TopPerformer]


# Update forward references
Team.update_forward_refs()
Player.update_forward_refs()
TopPerformingPlayer.update_forward_refs()
BoxScoreResponse.update_forward_refs()
