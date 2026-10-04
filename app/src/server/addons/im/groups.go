package im

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

const maxGroupMembers = 100

type imProblem struct {
	huma.ErrorModel
	Code string `json:"code"`
}

func newIMProblem(status int, code, detail string) error {
	return &imProblem{
		ErrorModel: huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: detail},
		Code:       code,
	}
}

type groupData struct {
	ID              uint   `json:"id"`
	Title           string `json:"title"`
	CreatorUID      uint   `json:"creatorUid"`
	WuKongChannelID string `json:"wukongChannelId"`
}

type memberData struct {
	UID         uint   `json:"uid"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	Avatar      string `json:"avatar"`
	AccountType string `json:"accountType"`
}

type groupOutput struct {
	Body envelope[groupData]
}

type groupsOutput struct {
	Body envelope[[]groupData]
}

type groupMembersData struct {
	Group   groupData    `json:"group"`
	Members []memberData `json:"members"`
}

type groupMembersOutput struct {
	Body envelope[groupMembersData]
}

type groupInput struct {
	Body struct {
		Title      string `json:"title"`
		MemberUIDs []uint `json:"memberUids"`
	}
}

type groupIDInput struct {
	ID string `path:"id"`
}

type groupMembersInput struct {
	ID   string `path:"id"`
	Body struct {
		MemberUIDs []uint `json:"memberUids"`
	}
}

type groupMemberInput struct {
	ID  string `path:"id"`
	UID string `path:"uid"`
}

type allowlistInput struct {
	ID       string `path:"id"`
	AgentUID string `path:"agentUid"`
	Body     struct {
		MemberUIDs []uint `json:"memberUids"`
	}
}

type allowlistGetInput struct {
	ID       string `path:"id"`
	AgentUID string `path:"agentUid"`
}

type allowlistData struct {
	GroupID       uint   `json:"groupId"`
	AgentUID      uint   `json:"agentUid"`
	OwnerUID      uint   `json:"ownerUid"`
	OwnerImplicit bool   `json:"ownerImplicit"`
	MemberUIDs    []uint `json:"memberUids"`
}

type allowlistOutput struct {
	Body envelope[allowlistData]
}

type emptyOutput struct {
	Body envelope[any]
}

type managerSubscribersResponse struct {
	Items []struct {
		UID string `json:"uid"`
	} `json:"items"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor"`
}

func (p *Plugin) registerGroups(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imCreateGroup", Method: http.MethodPost, Path: "/api/v1/im/groups",
		Summary: "Create an IM group", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.createGroup)
	huma.Register(api, huma.Operation{
		OperationID: "imListGroups", Method: http.MethodGet, Path: "/api/v1/im/groups",
		Summary: "List the caller's IM groups", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.listGroups)
	huma.Register(api, huma.Operation{
		OperationID: "imAddGroupMembers", Method: http.MethodPost, Path: "/api/v1/im/groups/{id}/members",
		Summary: "Add IM group members", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.addGroupMembers)
	huma.Register(api, huma.Operation{
		OperationID: "imRemoveGroupMember", Method: http.MethodDelete, Path: "/api/v1/im/groups/{id}/members/{uid}",
		Summary: "Remove or leave an IM group", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.removeGroupMember)
	huma.Register(api, huma.Operation{
		OperationID: "imDeleteGroup", Method: http.MethodDelete, Path: "/api/v1/im/groups/{id}",
		Summary: "Delete an IM group", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.deleteGroup)
	huma.Register(api, huma.Operation{
		OperationID: "imListGroupMembers", Method: http.MethodGet, Path: "/api/v1/im/groups/{id}/members",
		Summary: "List IM group members", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.listGroupMembers)
	huma.Register(api, huma.Operation{
		OperationID: "imReplaceGroupAgentAllowlist", Method: http.MethodPut,
		Path:    "/api/v1/im/groups/{id}/agents/{agentUid}/allowlist",
		Summary: "Replace an agent invocation allowlist", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.replaceAllowlist)
	huma.Register(api, huma.Operation{
		OperationID: "imGetGroupAgentAllowlist", Method: http.MethodGet,
		Path:    "/api/v1/im/groups/{id}/agents/{agentUid}/allowlist",
		Summary: "Read an agent invocation allowlist", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, p.getAllowlist)
}

func (p *Plugin) createGroup(ctx context.Context, input *groupInput) (*groupOutput, error) {
	creator := ctx.Value(userIDKey).(uint)
	title := strings.TrimSpace(input.Body.Title)
	if title == "" || len(title) > 255 {
		return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "title must be between 1 and 255 bytes")
	}
	members, err := normalizedUIDs(input.Body.MemberUIDs, maxGroupMembers)
	if err != nil {
		return nil, err
	}
	members = appendUnique(members, creator)
	if len(members) > maxGroupMembers {
		return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "member list is too large")
	}
	if err := validateUsers(ctx, members, "member_not_found", "one or more members do not exist"); err != nil {
		return nil, err
	}
	channelID, err := newGroupChannelID()
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "group could not be created")
	}
	group := IMGroup{GroupID: channelID, Title: title, CreatorUID: creator}
	if global.PRISM_DB == nil || global.PRISM_DB.WithContext(ctx).Create(&group).Error != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "group could not be created")
	}
	subscribers := uidStrings(members)
	err = postWuKongWithAuth(ctx, p.apiAddr, "/channel", p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
		"channel_id": channelID, "channel_type": groupChannel, "reset": 1, "subscribers": subscribers,
	}, nil)
	if err != nil {
		_ = postWuKongWithAuth(context.WithoutCancel(ctx), p.apiAddr, "/channel/delete",
			p.wuKongAdminUser, p.wuKongAdminPassword,
			map[string]any{"channel_id": channelID, "channel_type": groupChannel}, nil)
		_ = global.PRISM_DB.Delete(&group).Error
		p.cleanupWuKongGroupSubscribers(context.WithoutCancel(ctx), channelID, members)
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	return groupResponse(group), nil
}

func (p *Plugin) listGroups(ctx context.Context, _ *struct{}) (*groupsOutput, error) {
	var groups []IMGroup
	if global.PRISM_DB == nil || global.PRISM_DB.WithContext(ctx).Order("id DESC").Find(&groups).Error != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "groups are unavailable")
	}
	caller := ctx.Value(userIDKey).(uint)
	out := make([]groupData, 0, len(groups))
	// ponytail: one Manager lookup per local group; add a membership index only when group counts require it.
	// A failed lookup skips that group: a WuKong wipe leaves im rows with no channel, and one dead
	// row must not fail every user's list. Only a Manager that answered nothing at all is a 503.
	answered, outage := false, false
	for i := range groups {
		members, err := p.wuKongGroupMembers(ctx, groups[i].GroupID)
		if err != nil {
			status := 0
			var upstream *httpStatusError
			if errors.As(err, &upstream) {
				status = upstream.status
			}
			outage = outage || status != http.StatusNotFound
			// Upstream errors can contain credentials or response bodies; log the status only.
			slog.Warn("im group skipped: wukong membership lookup failed",
				"group_id", groups[i].ID, "channel_id", groups[i].GroupID, "status", status)
			continue
		}
		answered = true
		if containsUID(members, caller) {
			out = append(out, groupDTO(groups[i]))
		}
	}
	if outage && !answered {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	return &groupsOutput{Body: envelope[[]groupData]{Code: 0, Message: "success", Data: out}}, nil
}

func (p *Plugin) addGroupMembers(ctx context.Context, input *groupMembersInput) (*groupMembersOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	if group.CreatorUID != ctx.Value(userIDKey).(uint) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "only the group creator may add members")
	}
	members, err := normalizedUIDs(input.Body.MemberUIDs, maxGroupMembers)
	if err != nil || len(members) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "memberUids must not be empty")
	}
	if err := validateUsers(ctx, members, "member_not_found", "one or more members do not exist"); err != nil {
		return nil, err
	}
	current, err := p.wuKongGroupMembers(ctx, group.GroupID)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	for _, uid := range members {
		if containsUID(current, uid) {
			return nil, newIMProblem(http.StatusConflict, "group_conflict", "one or more users are already group members")
		}
	}
	if len(current)+len(members) > maxGroupMembers {
		return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "group member limit exceeded")
	}
	if err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/subscriber_add",
		p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
			"channel_id": group.GroupID, "channel_type": groupChannel, "subscribers": uidStrings(members),
		}, nil); err != nil {
		_ = postWuKongWithAuth(context.WithoutCancel(ctx), p.apiAddr, "/channel/subscriber_remove",
			p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
				"channel_id": group.GroupID, "channel_type": groupChannel, "subscribers": uidStrings(members),
			}, nil)
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	return p.groupMembersResponse(ctx, group, append(current, members...))
}

func (p *Plugin) removeGroupMember(ctx context.Context, input *groupMemberInput) (*emptyOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	memberUID, err := parseID(input.UID)
	if err != nil {
		return nil, err
	}
	caller := ctx.Value(userIDKey).(uint)
	if caller != group.CreatorUID && caller != memberUID {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "members may only leave the group themselves")
	}
	if memberUID == group.CreatorUID {
		return nil, newIMProblem(http.StatusConflict, "creator_transfer_or_delete_required",
			"the creator must transfer ownership or delete the group")
	}
	current, upstreamErr := p.wuKongGroupMembers(ctx, group.GroupID)
	if upstreamErr != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if caller != group.CreatorUID && !containsUID(current, caller) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "group membership is required")
	}
	if !containsUID(current, memberUID) {
		return nil, newIMProblem(http.StatusNotFound, "member_not_found", "group member not found")
	}
	if err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/subscriber_remove",
		p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
			"channel_id": group.GroupID, "channel_type": groupChannel,
			"subscribers": []string{strconv.FormatUint(uint64(memberUID), 10)},
		}, nil); err != nil {
		_ = postWuKongWithAuth(context.WithoutCancel(ctx), p.apiAddr, "/channel/subscriber_add",
			p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
				"channel_id": group.GroupID, "channel_type": groupChannel,
				"subscribers": []string{strconv.FormatUint(uint64(memberUID), 10)},
			}, nil)
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if err := global.PRISM_DB.WithContext(ctx).
		Where("group_id = ? AND (member_uid = ? OR agent_uid = ?)", group.GroupID, memberUID, memberUID).
		Delete(&IMGroupAgentAllowlist{}).Error; err != nil {
		_ = postWuKongWithAuth(context.WithoutCancel(ctx), p.apiAddr, "/channel/subscriber_add",
			p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
				"channel_id": group.GroupID, "channel_type": groupChannel,
				"subscribers": []string{strconv.FormatUint(uint64(memberUID), 10)},
			}, nil)
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "group permissions could not be updated")
	}
	return &emptyOutput{Body: envelope[any]{Code: 0, Message: "success", Data: nil}}, nil
}

func (p *Plugin) deleteGroup(ctx context.Context, input *groupIDInput) (*emptyOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	if group.CreatorUID != ctx.Value(userIDKey).(uint) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "only the group creator may delete the group")
	}
	members, err := p.wuKongGroupMembers(ctx, group.GroupID)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/delete",
		p.wuKongAdminUser, p.wuKongAdminPassword,
		map[string]any{"channel_id": group.GroupID, "channel_type": groupChannel}, nil); err != nil {
		p.restoreWuKongGroup(context.WithoutCancel(ctx), group.GroupID, members)
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if err := global.PRISM_DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("group_id = ?", group.GroupID).Delete(&IMGroupAgentAllowlist{}).Error; err != nil {
			return err
		}
		return tx.Delete(&group).Error
	}); err != nil {
		p.restoreWuKongGroup(context.WithoutCancel(ctx), group.GroupID, members)
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "group metadata could not be deleted")
	}
	p.cleanupWuKongGroupSubscribers(context.WithoutCancel(ctx), group.GroupID, members)
	return &emptyOutput{Body: envelope[any]{Code: 0, Message: "success", Data: nil}}, nil
}

func (p *Plugin) cleanupWuKongGroupSubscribers(ctx context.Context, channelID string, members []uint) {
	if len(members) == 0 {
		return
	}
	// WuKong deletion only disbands; Manager removal also clears membership indexes.
	// ponytail: best-effort cleanup; add durable retries if operational residue warrants it.
	body, _ := json.Marshal(map[string][]string{"uids": uidStrings(members)})
	response, err := p.managerRequest(ctx, http.MethodPost,
		p.managerAddr+"/manager/channels/2/"+url.PathEscape(channelID)+"/subscribers/remove", body)
	if err != nil {
		// Upstream errors can contain credentials or response bodies.
		slog.Warn("wukong group subscriber cleanup failed", "channel_id", channelID)
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		slog.Warn("wukong group subscriber cleanup failed", "channel_id", channelID, "status", response.StatusCode)
	}
}

func (p *Plugin) listGroupMembers(ctx context.Context, input *groupIDInput) (*groupMembersOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	members, err := p.wuKongGroupMembers(ctx, group.GroupID)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if !containsUID(members, ctx.Value(userIDKey).(uint)) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "group membership is required")
	}
	return p.groupMembersResponse(ctx, group, members)
}

func (p *Plugin) replaceAllowlist(ctx context.Context, input *allowlistInput) (*allowlistOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	if group.CreatorUID != ctx.Value(userIDKey).(uint) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "only the group creator may manage allowlists")
	}
	agentUID, err := parseID(input.AgentUID)
	if err != nil {
		return nil, err
	}
	if err := validateAgent(ctx, agentUID); err != nil {
		return nil, err
	}
	members, err := normalizedUIDs(input.Body.MemberUIDs, maxGroupMembers)
	if err != nil {
		return nil, err
	}
	if len(members) != 0 {
		if err := validateUsers(ctx, members, "member_not_found", "one or more members do not exist"); err != nil {
			return nil, err
		}
	}
	current, err := p.wuKongGroupMembers(ctx, group.GroupID)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "group membership service is unavailable")
	}
	if !containsUID(current, agentUID) {
		return nil, newIMProblem(http.StatusNotFound, "agent_not_found", "agent is not a group member")
	}
	for _, uid := range members {
		if !containsUID(current, uid) {
			return nil, newIMProblem(http.StatusNotFound, "member_not_found", "one or more users are not group members")
		}
	}
	members = removeUID(members, group.CreatorUID)
	if err := global.PRISM_DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("group_id = ? AND agent_uid = ?", group.GroupID, agentUID).
			Delete(&IMGroupAgentAllowlist{}).Error; err != nil {
			return err
		}
		for _, uid := range members {
			if err := tx.Create(&IMGroupAgentAllowlist{
				GroupID: group.GroupID, AgentUID: agentUID, MemberUID: uid, ManagedByOwnerUID: group.CreatorUID,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "allowlist could not be updated")
	}
	return allowlistResponse(group, agentUID, members), nil
}

func (p *Plugin) getAllowlist(ctx context.Context, input *allowlistGetInput) (*allowlistOutput, error) {
	group, err := loadGroup(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	if group.CreatorUID != ctx.Value(userIDKey).(uint) {
		return nil, newIMProblem(http.StatusForbidden, "group_forbidden", "only the group creator may read allowlists")
	}
	agentUID, err := parseID(input.AgentUID)
	if err != nil {
		return nil, err
	}
	if err := validateAgent(ctx, agentUID); err != nil {
		return nil, err
	}
	var rows []IMGroupAgentAllowlist
	if err := global.PRISM_DB.WithContext(ctx).
		Where("group_id = ? AND agent_uid = ?", group.GroupID, agentUID).
		Order("member_uid ASC").Find(&rows).Error; err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "allowlist is unavailable")
	}
	members := make([]uint, len(rows))
	for i := range rows {
		members[i] = rows[i].MemberUID
	}
	return allowlistResponse(group, agentUID, members), nil
}

func loadGroup(ctx context.Context, rawID string) (IMGroup, error) {
	id, err := parseID(rawID)
	if err != nil {
		return IMGroup{}, err
	}
	var group IMGroup
	if global.PRISM_DB == nil {
		return group, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "groups are unavailable")
	}
	err = global.PRISM_DB.WithContext(ctx).First(&group, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return group, newIMProblem(http.StatusNotFound, "group_not_found", "group not found")
	}
	if err != nil {
		return group, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "groups are unavailable")
	}
	return group, nil
}

func parseID(raw string) (uint, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || value == 0 {
		return 0, newIMProblem(http.StatusBadRequest, "invalid_request", "identifier must be a positive integer")
	}
	return uint(value), nil
}

func normalizedUIDs(raw []uint, limit int) ([]uint, error) {
	if len(raw) > limit {
		return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "member list is too large")
	}
	seen := make(map[uint]struct{}, len(raw))
	out := make([]uint, 0, len(raw))
	for _, uid := range raw {
		if uid == 0 {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "member identifiers must be positive")
		}
		if _, ok := seen[uid]; ok {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "member identifiers must be unique")
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func validateUsers(ctx context.Context, ids []uint, code, detail string) error {
	var count int64
	if global.PRISM_DB == nil || global.PRISM_DB.WithContext(ctx).Model(&authmodel.User{}).
		Where("id IN ? AND enable = ?", ids, 1).Count(&count).Error != nil {
		return newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "user validation is unavailable")
	}
	if count != int64(len(ids)) {
		return newIMProblem(http.StatusNotFound, code, detail)
	}
	return nil
}

func validateAgent(ctx context.Context, uid uint) error {
	var count int64
	if global.PRISM_DB == nil || global.PRISM_DB.WithContext(ctx).Model(&authmodel.User{}).
		Where("id = ? AND enable = ? AND account_type = ?", uid, 1, authmodel.AccountTypeAgent).
		Count(&count).Error != nil {
		return newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "agent validation is unavailable")
	}
	if count == 0 {
		return newIMProblem(http.StatusNotFound, "agent_not_found", "agent not found")
	}
	return nil
}

func (p *Plugin) groupMembersResponse(
	ctx context.Context, group IMGroup, memberUIDs []uint,
) (*groupMembersOutput, error) {
	sort.Slice(memberUIDs, func(i, j int) bool { return memberUIDs[i] < memberUIDs[j] })
	var users []authmodel.User
	if len(memberUIDs) != 0 {
		if err := global.PRISM_DB.WithContext(ctx).Where("id IN ?", memberUIDs).Find(&users).Error; err != nil {
			return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "group members are unavailable")
		}
	}
	byID := make(map[uint]authmodel.User, len(users))
	for i := range users {
		byID[users[i].ID] = users[i]
	}
	members := make([]memberData, 0, len(memberUIDs))
	for _, uid := range memberUIDs {
		user, ok := byID[uid]
		if !ok {
			continue
		}
		displayName := strings.TrimSpace(user.NickName)
		if displayName == "" {
			displayName = user.Username
		}
		members = append(members, memberData{
			UID: uid, Username: user.Username, DisplayName: displayName,
			Avatar: user.HeaderImg, AccountType: user.AccountType,
		})
	}
	return &groupMembersOutput{Body: envelope[groupMembersData]{
		Code: 0, Message: "success",
		Data: groupMembersData{Group: groupDTO(group), Members: members},
	}}, nil
}

func (p *Plugin) wuKongGroupMembers(ctx context.Context, channelID string) ([]uint, error) {
	cursor := ""
	seenCursors := map[string]bool{"": true}
	var members []uint
	for {
		endpoint, err := url.Parse(p.managerAddr + "/manager/channels/2/" +
			url.PathEscape(channelID) + "/subscribers")
		if err != nil {
			return nil, err
		}
		query := endpoint.Query()
		query.Set("limit", "500")
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		endpoint.RawQuery = query.Encode()
		response, err := p.managerGet(ctx, endpoint.String())
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			return nil, &httpStatusError{
				status: response.StatusCode, code: "wukong_manager_http_" + strconv.Itoa(response.StatusCode),
			}
		}
		var page managerSubscribersResponse
		err = json.NewDecoder(io.LimitReader(response.Body, 2*1024*1024)).Decode(&page)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			uid, parseErr := strconv.ParseUint(item.UID, 10, 64)
			if parseErr == nil && uid != 0 {
				members = appendUnique(members, uint(uid))
			}
		}
		if !page.HasMore {
			break
		}
		if seenCursors[page.NextCursor] {
			return nil, errors.New("wukong manager returned an invalid pagination cursor")
		}
		cursor = page.NextCursor
		seenCursors[cursor] = true
	}
	return members, nil
}

func (p *Plugin) restoreWuKongGroup(ctx context.Context, channelID string, members []uint) {
	_ = postWuKongWithAuth(ctx, p.apiAddr, "/channel", p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
		"channel_id": channelID, "channel_type": groupChannel, "reset": 1, "subscribers": uidStrings(members),
	}, nil)
}

func newGroupChannelID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "g-" + hex.EncodeToString(value[:]), nil
}

func groupResponse(group IMGroup) *groupOutput {
	return &groupOutput{Body: envelope[groupData]{Code: 0, Message: "success", Data: groupDTO(group)}}
}

func allowlistResponse(group IMGroup, agentUID uint, members []uint) *allowlistOutput {
	return &allowlistOutput{Body: envelope[allowlistData]{
		Code: 0, Message: "success",
		Data: allowlistData{
			GroupID: group.ID, AgentUID: agentUID, OwnerUID: group.CreatorUID,
			OwnerImplicit: true, MemberUIDs: members,
		},
	}}
}

func groupDTO(group IMGroup) groupData {
	return groupData{
		ID: group.ID, Title: group.Title, CreatorUID: group.CreatorUID, WuKongChannelID: group.GroupID,
	}
}

func uidStrings(ids []uint) []string {
	out := make([]string, len(ids))
	for i := range ids {
		out[i] = strconv.FormatUint(uint64(ids[i]), 10)
	}
	return out
}

func appendUnique(ids []uint, uid uint) []uint {
	if !containsUID(ids, uid) {
		return append(ids, uid)
	}
	return ids
}

func containsUID(ids []uint, uid uint) bool {
	for _, candidate := range ids {
		if candidate == uid {
			return true
		}
	}
	return false
}

func removeUID(ids []uint, uid uint) []uint {
	out := ids[:0]
	for _, candidate := range ids {
		if candidate != uid {
			out = append(out, candidate)
		}
	}
	return out
}
