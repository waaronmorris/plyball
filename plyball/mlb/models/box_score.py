from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class SpringLeague(BaseModel):
    id: int
    name: str
    link: str
    abbreviation: str


class Venue(BaseModel):
    id: int
    name: str
    link: str


class SpringVenue(BaseModel):
    id: int
    link: str


class League(BaseModel):
    id: int
    name: str
    link: str


class Division(BaseModel):
    id: int
    name: str
    link: str


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
    springLeagueGamesBack: str
    sportGamesBack: str
    divisionGamesBack: str
    conferenceGamesBack: str
    leagueRecord: LeagueRecord
    records: Dict[str, Any]
    divisionLeader: bool
    wins: int
    losses: int
    winningPercentage: str


class TeamData(BaseModel):
    springLeague: SpringLeague
    allStarStatus: str
    id: int
    name: str
    link: str
    season: int
    venue: Venue
    springVenue: SpringVenue
    teamCode: str
    fileCode: str
    abbreviation: str
    teamName: str
    locationName: str
    firstYearOfPlay: str
    league: League
    division: Division
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
    jerseyNumber: str
    position: PlayerPosition
    status: PlayerStatus
    parentTeamId: int
    battingOrder: Optional[str] = None
    stats: PlayerGameStats
    seasonStats: PlayerSeasonStats
    gameStatus: PlayerGameStatus
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
    label: str
    value: Optional[str] = None


class TopPerformingPlayer(BaseModel):
    person: Person
    jerseyNumber: str
    position: PlayerPosition
    status: PlayerStatus
    parentTeamId: int
    battingOrder: Optional[str] = None
    stats: PlayerGameStats
    seasonStats: PlayerSeasonStats
    gameStatus: PlayerGameStatus
    allPositions: Optional[List[AllPosition]] = None


class TopPerformer(BaseModel):
    player: TopPerformingPlayer
    type: str
    gameScore: int
    hittingGameScore: Optional[int] = None
    pitchingGameScore: Optional[int] = None


class BoxScoreResponse(BaseModel):
    copyright: str
    teams: Teams
    officials: List[Official]
    info: List[BoxScoreInfo]
    pitchingNotes: List[str]
    topPerformers: List[TopPerformer]


# Update forward references
Team.update_forward_refs()
Player.update_forward_refs()
TopPerformingPlayer.update_forward_refs()
BoxScoreResponse.update_forward_refs()
