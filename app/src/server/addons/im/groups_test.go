package im

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
)

type fakeWuKong struct {
	t        *testing.T
	server   *httptest.Server
	mu       sync.Mutex
	members  map[string][]uint
	failPath string
	getFail  map[string]int // channel -> Manager GET status (404 = channel wiped)
	deletes  int
	removes  int
}

func newFakeWuKong(t *testing.T) *fakeWuKong {
	t.Helper()
	fake := &fakeWuKong{t: t, members: map[string][]uint{}}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeWuKong) serveHTTP(response http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/manager/channels/2/"):
		parts := strings.Split(request.URL.Path, "/")
		channelID := parts[len(parts)-2]
		if status := f.getFail[channelID]; status != 0 {
			response.WriteHeader(status)
			return
		}
		items := make([]map[string]string, len(f.members[channelID]))
		for i, uid := range f.members[channelID] {
			items[i] = map[string]string{"uid": strconv.FormatUint(uint64(uid), 10)}
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"items": items, "has_more": false, "next_cursor": "",
		})
	case request.Method == http.MethodPost:
		var body struct {
			ChannelID   string   `json:"channel_id"`
			Subscribers []string `json:"subscribers"`
			UIDs        []string `json:"uids"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			f.t.Errorf("decode WuKong request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.HasPrefix(request.URL.Path, "/manager/channels/2/") &&
			strings.HasSuffix(request.URL.Path, "/subscribers/remove") {
			f.removes++
			if request.URL.Path == f.failPath {
				response.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			channelID := strings.Split(request.URL.Path, "/")[4]
			for _, raw := range body.UIDs {
				uid, _ := strconv.ParseUint(raw, 10, 64)
				f.members[channelID] = removeUID(f.members[channelID], uint(uid))
			}
			response.WriteHeader(http.StatusOK)
			return
		}
		subscribers := make([]uint, 0, len(body.Subscribers))
		for _, raw := range body.Subscribers {
			uid, _ := strconv.ParseUint(raw, 10, 64)
			subscribers = append(subscribers, uint(uid))
		}
		switch request.URL.Path {
		case "/channel":
			f.members[body.ChannelID] = subscribers
		case "/channel/subscriber_add":
			for _, uid := range subscribers {
				f.members[body.ChannelID] = appendUnique(f.members[body.ChannelID], uid)
			}
		case "/channel/subscriber_remove":
			for _, uid := range subscribers {
				f.members[body.ChannelID] = removeUID(f.members[body.ChannelID], uid)
			}
		case "/channel/delete":
			f.deletes++
			// WuKong marks the channel disbanded but retains its subscribers.
		default:
			response.WriteHeader(http.StatusNotFound)
			return
		}
		if request.URL.Path == f.failPath {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeWuKong) plugin() *Plugin {
	return &Plugin{apiAddr: f.server.URL, managerAddr: f.server.URL}
}

func groupRouter(t *testing.T, plugin *Plugin, authenticated bool) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	if authenticated {
		router.Use(func(c *gin.Context) {
			uid, _ := strconv.ParseUint(c.GetHeader("X-Test-User"), 10, 64)
			c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), userIDKey, uint(uid)))
			c.Next()
		})
	} else {
		sharedAuthStack(t, router)
	}
	api := humagin.New(router, huma.DefaultConfig("group test", "1"))
	plugin.registerGroups(api)
	return router
}

func groupRequest(
	t *testing.T, router http.Handler, method, path string, user uint, body any,
) *httptest.ResponseRecorder {
	t.Helper()
	var encoded bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&encoded).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, &encoded)
	request.Header.Set("Content-Type", "application/json")
	if user != 0 {
		request.Header.Set("X-Test-User", strconv.FormatUint(uint64(user), 10))
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func responseCode(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response %d %q: %v", response.Code, response.Body.String(), err)
	}
	return body.Code
}

func createGroupForTest(
	t *testing.T, router http.Handler, title string, members ...uint,
) groupData {
	t.Helper()
	response := groupRequest(t, router, http.MethodPost, "/api/v1/im/groups", 1, map[string]any{
		"title": title, "memberUids": members,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var body envelope[groupData]
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Data
}

func TestGroupLifecycleAndCreatorPolicy(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2, 3, 4} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	group := createGroupForTest(t, router, "Team", 2)

	response := groupRequest(t, router, http.MethodGet, "/api/v1/im/groups", 2, nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"title":"Team"`) {
		t.Fatalf("list response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodGet,
		fmt.Sprintf("/api/v1/im/groups/%d/members", group.ID), 2, nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"uid":1`) ||
		!strings.Contains(response.Body.String(), `"uid":2`) {
		t.Fatalf("members response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodPost,
		fmt.Sprintf("/api/v1/im/groups/%d/members", group.ID), 1,
		map[string]any{"memberUids": []uint{3, 4}})
	if response.Code != http.StatusOK {
		t.Fatalf("add response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d/members/4", group.ID), 1, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("creator remove response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d/members/2", group.ID), 2, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("leave response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d/members/3", group.ID), 2, nil)
	if response.Code != http.StatusForbidden || responseCode(t, response) != "group_forbidden" {
		t.Fatalf("non-creator response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d/members/1", group.ID), 3, nil)
	if response.Code != http.StatusForbidden || responseCode(t, response) != "group_forbidden" {
		t.Fatalf("member removing creator response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d/members/1", group.ID), 1, nil)
	if response.Code != http.StatusConflict ||
		responseCode(t, response) != "creator_transfer_or_delete_required" {
		t.Fatalf("creator leave response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodPost,
		fmt.Sprintf("/api/v1/im/groups/%d/members", group.ID), 1,
		map[string]any{"memberUids": []uint{3}})
	if response.Code != http.StatusConflict || responseCode(t, response) != "group_conflict" {
		t.Fatalf("duplicate add response=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d", group.ID), 1, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("delete response=%d %s", response.Code, response.Body.String())
	}
	var count, memberRows int64
	if err := db.Model(&IMGroup{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("groups after delete=%d err=%v", count, err)
	}
	if err := db.Model(&IMGroupMember{}).Count(&memberRows).Error; err != nil || memberRows != 0 {
		t.Fatalf("member rows after delete=%d err=%v", memberRows, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if members := fake.members[group.WuKongChannelID]; len(members) != 0 || fake.removes != 1 {
		t.Fatalf("subscribers after delete=%v removal calls=%d", members, fake.removes)
	}
}

// UNI-IM-DB W1 named check: with the WuKong Manager answering 404 for every channel,
// list, members and allowlist still answer from im_group_members, and non-members are refused.
func TestGroupReadsAnswerFromDBWithManagerDown(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2, 3} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	team := createGroupForTest(t, router, "Team", 2, 42)
	solo := createGroupForTest(t, router, "Solo")
	fake.mu.Lock()
	fake.getFail = map[string]int{team.WuKongChannelID: 404, solo.WuKongChannelID: 404}
	fake.mu.Unlock()

	get := func(path string, user uint) (int, string) {
		response := groupRequest(t, router, http.MethodGet, path, user, nil)
		return response.Code, response.Body.String()
	}
	if code, body := get("/api/v1/im/groups", 2); code != 200 || !strings.Contains(body, `"title":"Team"`) ||
		strings.Contains(body, `"title":"Solo"`) {
		t.Fatalf("member list=%d %s", code, body)
	}
	if code, body := get("/api/v1/im/groups", 3); code != 200 || !strings.Contains(body, `"data":[]`) {
		t.Fatalf("non-member list=%d %s", code, body)
	}
	membersPath := fmt.Sprintf("/api/v1/im/groups/%d/members", team.ID)
	if code, body := get(membersPath, 2); code != 200 || !strings.Contains(body, `"uid":1`) ||
		!strings.Contains(body, `"uid":42`) || strings.Contains(body, `"uid":3`) {
		t.Fatalf("members=%d %s", code, body)
	}
	if code, body := get(membersPath, 3); code != http.StatusForbidden || !strings.Contains(body, "group_forbidden") {
		t.Fatalf("non-member members=%d %s", code, body)
	}
	allowPath := fmt.Sprintf("/api/v1/im/groups/%d/agents/42/allowlist", team.ID)
	response := groupRequest(t, router, http.MethodPut, allowPath, 1, map[string]any{"memberUids": []uint{2}})
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"memberUids":[2]`) {
		t.Fatalf("allowlist=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodPut, allowPath, 1, map[string]any{"memberUids": []uint{3}})
	if response.Code != http.StatusNotFound || responseCode(t, response) != "member_not_found" {
		t.Fatalf("allowlist non-member=%d %s", response.Code, response.Body.String())
	}
	soloAllow := fmt.Sprintf("/api/v1/im/groups/%d/agents/42/allowlist", solo.ID)
	response = groupRequest(t, router, http.MethodPut, soloAllow, 1, map[string]any{"memberUids": []uint{}})
	if response.Code != http.StatusNotFound || responseCode(t, response) != "agent_not_found" {
		t.Fatalf("allowlist agent outside group=%d %s", response.Code, response.Body.String())
	}
	// Mutations keep the table in step: removing 2 drops them from list and members.
	if response := groupRequest(t, router, http.MethodDelete, membersPath+"/2", 1, nil); response.Code != 200 {
		t.Fatalf("remove=%d %s", response.Code, response.Body.String())
	}
	if code, body := get("/api/v1/im/groups", 2); code != 200 || !strings.Contains(body, `"data":[]`) {
		t.Fatalf("removed member list=%d %s", code, body)
	}
	if members, err := groupMembers(t.Context(), team.WuKongChannelID); err != nil || len(members) != 2 ||
		members[0] != 1 || members[1] != 42 {
		t.Fatalf("db members=%v err=%v", members, err)
	}
}

// The one-time fill: a fixture Manager payload lands in im_group_members, a wiped channel
// (404) is seeded with its creator, a Manager outage leaves the group for the next boot,
// and a re-run neither duplicates rows nor asks the Manager about filled groups again.
func TestFillGroupMembersFromManager(t *testing.T) {
	db := m2DB(t)
	gets := map[string]int{}
	status := map[string]int{"g-wiped": 404, "g-down": 502}
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelID := strings.Split(r.URL.Path, "/")[4]
		gets[channelID]++
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/subscribers") {
			t.Errorf("unexpected Manager call %s %s", r.Method, r.URL.Path)
		}
		if code := status[channelID]; code != 0 {
			w.WriteHeader(code)
			return
		}
		// Fixture shaped like the beta.21 Manager subscribers page, including a junk uid.
		fmt.Fprint(w, `{"items":[{"uid":"7"},{"uid":"9"},{"uid":"not-a-uid"},{"uid":"7"}],"has_more":false,"next_cursor":""}`)
	}))
	defer manager.Close()
	for _, group := range []IMGroup{
		{GroupID: "g-live", CreatorUID: 7}, {GroupID: "g-wiped", CreatorUID: 5}, {GroupID: "g-down", CreatorUID: 6},
	} {
		if err := db.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
	}
	p := &Plugin{managerAddr: manager.URL}
	want := map[string][]uint{"g-live": {7, 9}, "g-wiped": {5}, "g-down": {}}
	check := func(label string) {
		t.Helper()
		for groupID, expected := range want {
			members, err := groupMembers(t.Context(), groupID)
			if err != nil || fmt.Sprint(members) != fmt.Sprint(expected) {
				t.Fatalf("%s %s members=%v err=%v want %v", label, groupID, members, err, expected)
			}
		}
	}

	p.fillGroupMembers(t.Context(), db)
	check("first fill")
	p.fillGroupMembers(t.Context(), db)
	check("re-fill")
	var rows int64
	if err := db.Model(&IMGroupMember{}).Count(&rows).Error; err != nil || rows != 3 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	if gets["g-live"] != 1 || gets["g-wiped"] != 1 || gets["g-down"] != 2 {
		t.Fatalf("Manager GETs=%v: filled groups must not be re-read, failed ones must be retried", gets)
	}
	// The Manager recovers: the next boot fills the group it missed.
	delete(status, "g-down")
	want["g-down"] = []uint{6, 7, 9}
	p.fillGroupMembers(t.Context(), db)
	check("after recovery")
}

func TestGroupWuKongFailureCompensation(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db := m2DB(t)
		addUser(t, db, 1, authmodel.AccountTypeHuman)
		addUser(t, db, 2, authmodel.AccountTypeHuman)
		fake := newFakeWuKong(t)
		fake.failPath = "/channel"
		response := groupRequest(t, groupRouter(t, fake.plugin(), true), http.MethodPost,
			"/api/v1/im/groups", 1, map[string]any{"title": "Fail", "memberUids": []uint{2}})
		if response.Code != http.StatusServiceUnavailable ||
			responseCode(t, response) != "wukong_unavailable" {
			t.Fatalf("create failure=%d %s", response.Code, response.Body.String())
		}
		var count, memberRows int64
		if err := db.Model(&IMGroup{}).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("local groups=%d err=%v", count, err)
		}
		if err := db.Model(&IMGroupMember{}).Count(&memberRows).Error; err != nil || memberRows != 0 {
			t.Fatalf("local member rows=%d err=%v", memberRows, err)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for channel, members := range fake.members {
			if len(members) != 0 {
				t.Fatalf("orphan subscribers for %s: %v", channel, members)
			}
		}
		if fake.deletes != 1 || fake.removes != 1 {
			t.Fatalf("WuKong state=%v deletes=%d", fake.members, fake.deletes)
		}
	})

	t.Run("delete", func(t *testing.T) {
		db := m2DB(t)
		addUser(t, db, 1, authmodel.AccountTypeHuman)
		addUser(t, db, 2, authmodel.AccountTypeHuman)
		addUser(t, db, 42, authmodel.AccountTypeAgent)
		fake := newFakeWuKong(t)
		router := groupRouter(t, fake.plugin(), true)
		group := createGroupForTest(t, router, "Keep", 2, 42)
		if err := db.Create(&IMGroupAgentAllowlist{
			GroupID: group.WuKongChannelID, AgentUID: 42, MemberUID: 2, ManagedByOwnerUID: 1,
		}).Error; err != nil {
			t.Fatal(err)
		}
		fake.failPath = "/channel/delete"
		response := groupRequest(t, router, http.MethodDelete,
			fmt.Sprintf("/api/v1/im/groups/%d", group.ID), 1, nil)
		if response.Code != http.StatusServiceUnavailable ||
			responseCode(t, response) != "wukong_unavailable" {
			t.Fatalf("delete failure=%d %s", response.Code, response.Body.String())
		}
		var groupCount, allowCount int64
		_ = db.Model(&IMGroup{}).Count(&groupCount).Error
		_ = db.Model(&IMGroupAgentAllowlist{}).Count(&allowCount).Error
		if members, _ := groupMembers(t.Context(), group.WuKongChannelID); groupCount != 1 || allowCount != 1 ||
			len(members) != 3 {
			t.Fatalf("groups=%d allowlists=%d members=%v", groupCount, allowCount, members)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if !containsUID(fake.members[group.WuKongChannelID], 1) ||
			!containsUID(fake.members[group.WuKongChannelID], 2) {
			t.Fatalf("restored members=%v", fake.members[group.WuKongChannelID])
		}
	})

	t.Run("add and remove", func(t *testing.T) {
		db := m2DB(t)
		for _, id := range []uint{1, 2, 3} {
			addUser(t, db, id, authmodel.AccountTypeHuman)
		}
		fake := newFakeWuKong(t)
		router := groupRouter(t, fake.plugin(), true)
		group := createGroupForTest(t, router, "Rollback", 2)
		membersPath := fmt.Sprintf("/api/v1/im/groups/%d/members", group.ID)

		fake.failPath = "/channel/subscriber_add"
		response := groupRequest(t, router, http.MethodPost, membersPath, 1,
			map[string]any{"memberUids": []uint{3}})
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("add failure=%d %s", response.Code, response.Body.String())
		}
		fake.mu.Lock()
		if containsUID(fake.members[group.WuKongChannelID], 3) {
			t.Fatalf("add rollback members=%v", fake.members[group.WuKongChannelID])
		}
		fake.mu.Unlock()
		if ok, err := isGroupMember(t.Context(), group.WuKongChannelID, 3); err != nil || ok {
			t.Fatalf("add rollback db member=%v err=%v", ok, err)
		}

		fake.failPath = ""
		response = groupRequest(t, router, http.MethodPost, membersPath, 1,
			map[string]any{"memberUids": []uint{3}})
		if response.Code != http.StatusOK {
			t.Fatalf("setup add=%d %s", response.Code, response.Body.String())
		}
		fake.failPath = "/channel/subscriber_remove"
		response = groupRequest(t, router, http.MethodDelete, membersPath+"/3", 1, nil)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("remove failure=%d %s", response.Code, response.Body.String())
		}
		if ok, err := isGroupMember(t.Context(), group.WuKongChannelID, 3); err != nil || !ok {
			t.Fatalf("remove rollback db member=%v err=%v", ok, err)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if !containsUID(fake.members[group.WuKongChannelID], 3) {
			t.Fatalf("remove rollback members=%v", fake.members[group.WuKongChannelID])
		}
	})
}

func TestGroupDeleteSubscriberCleanupFailure(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	group := createGroupForTest(t, router, "Cleanup failure", 2)
	fake.failPath = "/manager/channels/2/" + group.WuKongChannelID + "/subscribers/remove"
	response := groupRequest(t, router, http.MethodDelete,
		fmt.Sprintf("/api/v1/im/groups/%d", group.ID), 1, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("delete response=%d %s", response.Code, response.Body.String())
	}
	var count int64
	if err := db.Model(&IMGroup{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("groups after delete=%d err=%v", count, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.deletes != 1 || fake.removes != 1 || len(fake.members[group.WuKongChannelID]) != 2 {
		t.Fatalf("deletes=%d removes=%d members=%v", fake.deletes, fake.removes, fake.members)
	}
}

func TestAllowlistReplaceAndOwnerImplicit(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2, 3} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	group := createGroupForTest(t, router, "Agents", 2, 3, 42)
	path := fmt.Sprintf("/api/v1/im/groups/%d/agents/42/allowlist", group.ID)

	response := groupRequest(t, router, http.MethodPut, path, 1,
		map[string]any{"memberUids": []uint{1, 2}})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"ownerImplicit":true`) ||
		!strings.Contains(response.Body.String(), `"memberUids":[2]`) {
		t.Fatalf("first replace=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodPut, path, 1,
		map[string]any{"memberUids": []uint{3}})
	if response.Code != http.StatusOK {
		t.Fatalf("second replace=%d %s", response.Code, response.Body.String())
	}
	response = groupRequest(t, router, http.MethodGet, path, 1, nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"memberUids":[3]`) {
		t.Fatalf("get allowlist=%d %s", response.Code, response.Body.String())
	}
	var rows []IMGroupAgentAllowlist
	if err := db.Find(&rows).Error; err != nil || len(rows) != 1 || rows[0].MemberUID != 3 {
		t.Fatalf("allowlist rows=%+v err=%v", rows, err)
	}
	response = groupRequest(t, router, http.MethodPut, path, 2,
		map[string]any{"memberUids": []uint{2}})
	if response.Code != http.StatusForbidden || responseCode(t, response) != "group_forbidden" {
		t.Fatalf("non-owner replace=%d %s", response.Code, response.Body.String())
	}
}

func TestGroupDomainErrorsAndUnauthenticated(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)

	tests := []struct {
		name   string
		method string
		path   string
		body   any
		status int
		code   string
	}{
		{"invalid", http.MethodPost, "/api/v1/im/groups", map[string]any{"title": "", "memberUids": []uint{}},
			http.StatusBadRequest, "invalid_request"},
		{"missing", http.MethodGet, "/api/v1/im/groups/999/members", nil,
			http.StatusNotFound, "group_not_found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := groupRequest(t, router, test.method, test.path, 1, test.body)
			if response.Code != test.status || responseCode(t, response) != test.code {
				t.Fatalf("response=%d %s", response.Code, response.Body.String())
			}
		})
	}

	// The same routes behind the real shared auth stack: anonymous is rejected
	// by the framework middleware, and a live session reaches the handler with
	// its user id bridged into the request context.
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	guarded := groupRouter(t, fake.plugin(), false)
	response := groupRequest(t, guarded, http.MethodGet, "/api/v1/im/groups", 0, nil)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d %s", response.Code, response.Body.String())
	}
	token := issueSessionToken(t, db, 1, "session-1")
	if recorder := authorizedRequest(t, guarded, http.MethodGet, "/api/v1/im/groups", token); recorder.Code != http.StatusOK {
		t.Fatalf("authenticated status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestIMProblemMapping(t *testing.T) {
	tests := []struct {
		status int
		code   string
	}{
		{http.StatusBadRequest, "invalid_request"},
		{http.StatusUnauthorized, "authentication_required"},
		{http.StatusForbidden, "group_forbidden"},
		{http.StatusNotFound, "group_not_found"},
		{http.StatusConflict, "group_conflict"},
		{http.StatusServiceUnavailable, "wukong_unavailable"},
	}
	for _, test := range tests {
		problem, ok := newIMProblem(test.status, test.code, "safe").(*imProblem)
		if !ok || problem.Status != test.status || problem.Code != test.code {
			t.Fatalf("mapping %d/%s = %#v", test.status, test.code, problem)
		}
	}
}
