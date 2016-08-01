import json
import logging
import time
import urllib.parse
from datetime import timedelta, datetime
from urllib.parse import urlencode

import dateutil.parser
import pytz
import requests
from django.core.urlresolvers import reverse

from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatisticUnit, Waypoint, WaypointType, \
    Location, \
    Lap, ActivityStatistic
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.sessioncache import SessionCache
from tapiriik.settings import WEB_ROOT, POWERTRAXX_CLIENT_ID, POWERTRAXX_CLIENT_SECRET

logger = logging.getLogger(__name__)


class PowerTraxxService(ServiceBase):
    ID = "powertraxx"
    DisplayName = "PowerTraxx"
    DisplayAbbreviation = "PT"
    AuthenticationType = ServiceAuthenticationType.OAuth
    SupportsHR = SupportsCadence = SupportsPower = True

    SupportsActivityDeletion = True
    AuthenticationNoFrame = True
    BaseUrl = "http://localhost:52060"

    # mapping powertraxx -> common
    _activityMappings = {
        "run": ActivityType.Running,
        "bicycle": ActivityType.Cycling,
        "racingbicycle": ActivityType.Cycling,
        "mountainbike": ActivityType.MountainBiking,
        "walking": ActivityType.Walking,
        "hike": ActivityType.Hiking,
        "snowboard": ActivityType.Snowboarding,
        "skialpin": ActivityType.DownhillSkiing,
        "classicskiing": ActivityType.CrossCountrySkiing,
        "skating": ActivityType.Skating,
        "swim": ActivityType.Swimming,
        "elliptical": ActivityType.Elliptical,
        "other": ActivityType.Other
    }

    # common -> powertraxx
    _reverseActivityMappings = {
        ActivityType.Running: 4,
        ActivityType.Cycling: 3,
        ActivityType.Walking: 9,
        ActivityType.MountainBiking: 2,
        ActivityType.Hiking: 5,
        ActivityType.CrossCountrySkiing: 12,
        ActivityType.DownhillSkiing: 13,
        ActivityType.Snowboarding: 22,
        ActivityType.Skating: 11,
        ActivityType.Swimming: 6,
        ActivityType.Elliptical: 10,
        ActivityType.Other: 99,
    }

    SupportedActivities = list(_reverseActivityMappings.keys())

    _tokenCache = SessionCache("powertraxx", lifetime=timedelta(minutes=29))

    def WebInit(self):

        params = {'scope': 'activity',
                  'client_id': POWERTRAXX_CLIENT_ID,
                  'state': 'ptr_api',
                  'response_type': 'code',
                  'redirect_uri': WEB_ROOT + reverse("oauth_return", kwargs={"service": "powertraxx"})}

        self.UserAuthorizationURL = self.BaseUrl + "/authorize?" + urlencode(params)

    def RetrieveAuthorizationToken(self, req, level):

        code = req.GET.get("code")

        params = {"grant_type": "authorization_code",
                  "code": code,
                  "client_id": POWERTRAXX_CLIENT_ID,
                  "client_secret": POWERTRAXX_CLIENT_SECRET,
                  "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "powertraxx"})}

        response = requests.post("%s/token" % self.BaseUrl, data=urllib.parse.urlencode(params),
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})

        if response.status_code != 200:
            print(response.text)
            raise APIException("Invalid code")

        tokenItem = self._getTokenFromResponse(response);

        userInfo = requests.post(self.BaseUrl + "/api/account/userinfo",
                                 headers={"Authorization": "Bearer %s" % tokenItem["AccessToken"]})
        uid = userInfo.json()["Id"]

        return (uid, tokenItem)

    def RevokeAuthorization(self, serviceRecord):
        pass  # Can't revoke these tokens

    def DeleteCachedData(self, serviceRecord):
        self._tokenCache.Delete(serviceRecord.ExternalID)

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        activities = []
        exclusions = []

        items = self._getActivities(serviceRecord, exhaustive)
        for item in items:
            id = item["Id"]
            logger.debug("Activity Id: " + id)
            activity = UploadedActivity()
            activity.ServiceData = {"ActivityID": id}
            _type = self._activityMappings.get(item['SportType'])
            if not _type:
                _type = ActivityType.Other

            activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=item["Distance"])
            activity.FallbackTZ = pytz.timezone("Europe/Paris")
            activity.Type = _type
            activity.StartTime = dateutil.parser.parse(item["StartDate"])
            activity.EndTime = dateutil.parser.parse(item["EndDate"])
            activity.Name = item["Name"]
            activity.CalculateUID()

            activities.append(activity)

        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        return self._downloadActivity(serviceRecord, activity)

    def UploadActivity(self, serviceRecord, activity):

        if not activity.GPS:
            activity_data = {"name": activity.Name,
                             "comment": activity.Notes,
                             "date": activity.StartTime.isoformat(),
                             "share": not activity.Private,
                             "duration": self._resolveDuration(activity),
                             "pause": self._resolvePause(activity),
                             "sportType": self._reverseActivityMappings[activity.Type]}
            if activity.Stats.Distance is not None:
                activity_data["distance"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value,

            headers = self._getAuthHeaders(serviceRecord)
            headers.update({"Content-Type": "application/json"})
            upload_resp = requests.post(self.BaseUrl + "/api/activity", data=json.dumps(activity_data),
                                        headers=headers)

            if upload_resp.status_code != 200:
                if upload_resp.status_code == 401:
                    raise APIException("ST.mobi trial expired", block=True,
                                       user_exception=UserException(UserExceptionType.AccountExpired,
                                                                    intervention_required=True))
                raise APIException("Unable to upload activity %s" % upload_resp.text)
            return upload_resp.json()["AcId"]

        else:
            activity_data = {
                "activity": {},
                "activityRawFormatList": [],
                "start_time": activity.StartTime.isoformat(),
                "share": not activity.Private
            }

            activity.GetFlatWaypoints()
            if activity.Name:
                activity_data["activity"]["name"] = activity.Name
            if activity.Notes:
                activity_data["activity"]["comment"] = activity.Notes
            if activity.Stationary:
                activity_data["activity"]["indoor"] = True

            activity_data["activity"]["sportType"] = self._reverseActivityMappings[activity.Type]

            for wp in activity.GetFlatWaypoints():
                item = {}
                if wp.Location and wp.Location.Latitude and wp.Location.Longitude:
                    item["lat"] = wp.Location.Latitude
                    item["lon"] = wp.Location.Longitude
                if wp.Location and wp.Location.Altitude:
                    item["ele"] = wp.Location.Altitude
                if wp.Timestamp:
                    item["timestampValue"] = time.mktime(wp.Timestamp.astimezone(pytz.utc).timetuple())
                if wp.HR:
                    item["heartrate"] = wp.HR
                if wp.Speed:
                    item["speed"] = wp.Speed
                if wp.Distance:
                    item["distance"] = wp.Distance
                if wp.Cadence:
                    item["cadence"] = wp.Cadence
                if wp.RunCadence:
                    item["steps"] = wp.RunCadence
                if wp.Power:
                    item["power"] = wp.Power

                activity_data["activityRawFormatList"].append(item)

        headers = self._getAuthHeaders(serviceRecord)
        headers.update({"Content-Type": "application/json"})
        upload_resp = requests.post(self.BaseUrl + "/api/activity", data=json.dumps(activity_data),
                                    headers=headers)
        if upload_resp.status_code != 200:
            if upload_resp.status_code == 401:
                raise APIException("ST.mobi trial expired", block=True,
                                   user_exception=UserException(UserExceptionType.AccountExpired,
                                                                intervention_required=True))
            raise APIException("Unable to upload activity %s" % upload_resp.text)
        return upload_resp.json()["AcId"]

    def DeleteActivity(self, serviceRecord, uploadId):
        headers = self._getAuthHeaders(serviceRecord)
        del_res = requests.delete(self.BaseUrl + "/api/activity/%s" % uploadId, headers=headers)
        del_res.raise_for_status()

    def _downloadActivity(self, serviceRecord, activity):
        activityId = activity.ServiceData["ActivityID"]
        headers = self._getAuthHeaders(serviceRecord)
        logger.debug("download activity with id: " + activityId)
        response = requests.get(self.BaseUrl + "/api/activity/" + activityId, headers=headers)
        if response.status_code != 200:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to download activity" + activityId, block=True,
                                   user_exception=UserException(UserExceptionType.Authorization,
                                                                intervention_required=True))
            raise APIException(
                "Unable to download activity " + activityId + " response " + str(response) + " " + response.text)

        activityData = response.json()
        if activityData["Comment"]:
            activity.Notes = activityData["Comment"]

        activity.GPS = activityData["HasGps"]
        activity.Private = not activityData["IsPublic"]

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]
        lastWaypoint = None;
        for wp in list(activityData["Points"]):
            waypoint = Waypoint(dateutil.parser.parse(wp["TimeStamp"]))
            waypoint.Distance = wp["Distance"]
            if wp["Pause"] is not None:
                waypoint.Type = WaypointType.Pause
            else:
                if lastWaypoint is not None and lastWaypoint.Type is WaypointType.Pause:
                    waypoint.Type = WaypointType.Resume
                else:
                    waypoint.Type = WaypointType.Regular

            waypoint.Location = Location(wp["Lat"], wp["Lon"], wp["Elevation"])
            waypoint.Power = wp["Power"]
            waypoint.Cadence = wp["Cadence"]
            waypoint.RunCadence = wp["Steps"]
            waypoint.Speed = wp["Speed"] / 3.6
            waypoint.HR = wp["Heartrate"]

            lap.Waypoints.append(waypoint)
            lastWaypoint = waypoint

        activity.Stationary = activity.CountTotalWaypoints() <= 1

        if not activity.Stationary:
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End

        logger.error("test " + activity.UID)
        return activity

    def _getAuthHeaders(self, serviceRecord):
        authInfo = self._tokenCache.Get(serviceRecord.ExternalID)
        token = None
        exp_date = None

        if authInfo:
            token = serviceRecord.Authorization["AccessToken"]
            exp_date = serviceRecord.Authorization["ExpirationDate"]

        if not token or exp_date < datetime.utcnow():
            logger.debug("no token or expired")

            # Use refresh token to get access token
            # Hardcoded return URI to get around the lack of URL reversing without loading up all the Django stuff
            params = {"grant_type": "refresh_token",
                      "refresh_token": serviceRecord.Authorization["RefreshToken"],
                      "client_id": POWERTRAXX_CLIENT_ID,
                      "client_secret": POWERTRAXX_CLIENT_SECRET}

            response = requests.post("{0}/token".format(self.BaseUrl), data=urlencode(params),
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})

            if response.status_code != 200:
                if response.status_code >= 400 and response.status_code < 500:
                    raise APIException(
                        "Could not retrieve refreshed token %s %s" % (response.status_code, response.text), block=True,
                        user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text))

            tokenItem = self._getTokenFromResponse(response)

            self._tokenCache.Set(serviceRecord.ExternalID, tokenItem)
            serviceRecord.Service.EnsureServiceRecordWithAuth(serviceRecord.ExternalID, tokenItem)

            token = tokenItem.AccessToken

        return {"Authorization": "Bearer %s" % token}

    def _getTokenFromResponse(self, response):
        expires_in = response.json()["expires_in"] - 30  # 30 seconds buffer so it doesn't expire mid request
        token = response.json()["access_token"]
        refresh_token = response.json()["refresh_token"]
        expiration_date = datetime.utcnow() + timedelta(seconds=expires_in)
        tokenItem = {
            "AccessToken": token,
            "RefreshToken": refresh_token,
            "ExpirationDate": expiration_date
        }
        return tokenItem

    def _resolveDuration(self, obj):
        if obj.Stats.TimerTime.Value is not None:
            return obj.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        if obj.Stats.MovingTime.Value is not None:
            return obj.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        return (obj.EndTime - obj.StartTime).total_seconds()

    def _resolvePause(self, obj):
        if obj.Stats.TimerTime.Value is not None and obj.Stats.MovingTime.Value is not None:
            return obj.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value - obj.Stats.MovingTime.asUnits(
                ActivityStatisticUnit.Seconds).Value

        return 0

    def _getActivities(self, serviceRecord, exhaustive=False):
        headers = self._getAuthHeaders(serviceRecord)
        if exhaustive:
            res = requests.get(self.BaseUrl + "/api/activity/list", headers=headers)
        else:
            res = requests.get(self.BaseUrl + "/api/activity/list?count=25", headers=headers)

        try:
            return res.json()
        except ValueError:
            raise APIException("Could not decode activity list response %s %s" % (res.status_code, res.text))