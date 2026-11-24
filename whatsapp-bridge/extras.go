package main

// Additions to the original bridge: real contact names, read state, marking as read and quoted replies.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// ---------- Names ----------

// resolveContactName gives the best name WhatsApp knows for a person: the address book name,
// then their first name, their WhatsApp name (push name), their business name. A LID (WhatsApp's
// hidden id) is looked up through its phone number too. Empty if nothing is known.
func resolveContactName(client *whatsmeow.Client, jid types.JID) string {
	ctx := context.Background()
	candidates := []types.JID{jid.ToNonAD()}
	if jid.Server == types.HiddenUserServer {
		if pn, err := client.Store.LIDs.GetPNForLID(ctx, jid.ToNonAD()); err == nil && !pn.IsEmpty() {
			candidates = append([]types.JID{pn}, candidates...)
		}
	} else if jid.Server == types.DefaultUserServer {
		if lid, err := client.Store.LIDs.GetLIDForPN(ctx, jid.ToNonAD()); err == nil && !lid.IsEmpty() {
			candidates = append(candidates, lid)
		}
	}
	for _, c := range candidates {
		info, err := client.Store.Contacts.GetContact(ctx, c)
		if err != nil || !info.Found {
			continue
		}
		for _, n := range []string{info.FullName, info.FirstName, info.PushName, info.BusinessName} {
			if n != "" {
				return n
			}
		}
	}
	return ""
}

var numericName = regexp.MustCompile(`^(Group )?[0-9]+$`)

// refreshChatNames names the chats still called by a number, once the contacts have arrived.
func refreshChatNames(client *whatsmeow.Client, store *MessageStore, logger waLog.Logger) {
	rows, err := store.db.Query("SELECT jid, COALESCE(name, '') FROM chats")
	if err != nil {
		logger.Warnf("refreshChatNames: %v", err)
		return
	}
	type chat struct{ jid, name string }
	var todo []chat
	for rows.Next() {
		var c chat
		if rows.Scan(&c.jid, &c.name) == nil && (c.name == "" || numericName.MatchString(c.name)) {
			todo = append(todo, c)
		}
	}
	rows.Close()

	renamed := 0
	for _, c := range todo {
		jid, err := types.ParseJID(c.jid)
		if err != nil {
			continue
		}
		name := ""
		if jid.Server == types.GroupServer {
			if info, err := client.GetGroupInfo(context.Background(), jid); err == nil {
				name = info.Name
			}
		} else {
			name = resolveContactName(client, jid)
		}
		if name != "" && name != c.name {
			if _, err := store.db.Exec("UPDATE chats SET name = ? WHERE jid = ?", name, c.jid); err == nil {
				renamed++
			}
		}
	}
	if renamed > 0 {
		fmt.Printf("Named %d chats from contacts\n", renamed)
	}
}

// nameRefresher runs refreshChatNames at start, every 10 minutes and shortly after contacts change.
type nameRefresher struct {
	mu      sync.Mutex
	pending bool
	kick    chan struct{}
}

func startNameRefresher(client *whatsmeow.Client, store *MessageStore, logger waLog.Logger) *nameRefresher {
	r := &nameRefresher{kick: make(chan struct{}, 1)}
	go func() {
		time.Sleep(5 * time.Second)
		refreshChatNames(client, store, logger)
		ticker := time.NewTicker(10 * time.Minute)
		for {
			select {
			case <-ticker.C:
			case <-r.kick:
				time.Sleep(20 * time.Second) // contacts arrive in bursts
			}
			r.mu.Lock()
			r.pending = false
			r.mu.Unlock()
			refreshChatNames(client, store, logger)
		}
	}()
	return r
}

func (r *nameRefresher) Kick() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.pending {
		r.pending = true
		select {
		case r.kick <- struct{}{}:
		default:
		}
	}
}

// ---------- Read state ----------

// ensureReadColumn adds chats.last_read_time: when the chat was last read on any of your devices.
func ensureReadColumn(store *MessageStore) {
	var n int
	store.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('chats') WHERE name = 'last_read_time'").Scan(&n)
	if n == 0 {
		store.db.Exec("ALTER TABLE chats ADD COLUMN last_read_time TIMESTAMP")
	}
}

func setChatRead(store *MessageStore, chatJID string, at time.Time) {
	store.db.Exec(`UPDATE chats SET last_read_time = ? WHERE jid = ?
		AND (last_read_time IS NULL OR last_read_time < ?)`, at, chatJID, at)
}

// handleOwnReceipt: a read receipt from one of your own devices means you read that chat there.
func handleOwnReceipt(store *MessageStore, evt *events.Receipt) {
	if !evt.IsFromMe || (evt.Type != types.ReceiptTypeRead && evt.Type != types.ReceiptTypeReadSelf) {
		return
	}
	setChatRead(store, evt.Chat.String(), evt.Timestamp)
}

// handleMarkChatAsRead: "mark as read / unread" done on another device.
func handleMarkChatAsRead(store *MessageStore, evt *events.MarkChatAsRead) {
	if evt.Action == nil {
		return
	}
	if evt.Action.GetRead() {
		setChatRead(store, evt.JID.String(), evt.Timestamp)
	} else {
		store.db.Exec("UPDATE chats SET last_read_time = NULL WHERE jid = ?", evt.JID.String())
	}
}

// senderJID rebuilds a sender's full JID from the user part the store keeps.
func senderJID(client *whatsmeow.Client, user string) types.JID {
	lid := types.NewJID(user, types.HiddenUserServer)
	if pn, err := client.Store.LIDs.GetPNForLID(context.Background(), lid); err == nil && !pn.IsEmpty() {
		return lid
	}
	return types.NewJID(user, types.DefaultUserServer)
}

type unreadMessage struct {
	id     string
	sender string
	at     time.Time
}

// unreadMessages: messages others sent after you last read or wrote in the chat.
func unreadMessages(store *MessageStore, chatJID string) ([]unreadMessage, error) {
	var lastRead, lastMine *time.Time
	store.db.QueryRow("SELECT last_read_time FROM chats WHERE jid = ?", chatJID).Scan(&lastRead)
	store.db.QueryRow("SELECT MAX(timestamp) FROM messages WHERE chat_jid = ? AND is_from_me = 1", chatJID).Scan(&lastMine)
	var since time.Time
	for _, t := range []*time.Time{lastRead, lastMine} {
		if t != nil && t.After(since) {
			since = *t
		}
	}
	rows, err := store.db.Query(`SELECT id, sender, timestamp FROM messages
		WHERE chat_jid = ? AND is_from_me = 0 ORDER BY timestamp DESC LIMIT 200`, chatJID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []unreadMessage
	for rows.Next() {
		var m unreadMessage
		if rows.Scan(&m.id, &m.sender, &m.at) == nil && m.at.After(since) {
			out = append(out, m)
		}
	}
	return out, nil
}

// markChatRead sends read receipts for a chat's unread messages and records it as read.
func markChatRead(client *whatsmeow.Client, store *MessageStore, chatJID string) (int, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return 0, fmt.Errorf("invalid chat JID: %v", err)
	}
	msgs, err := unreadMessages(store, chatJID)
	if err != nil {
		return 0, err
	}
	bySender := map[string][]types.MessageID{}
	for _, m := range msgs {
		bySender[m.sender] = append(bySender[m.sender], types.MessageID(m.id))
	}
	for user, ids := range bySender {
		sender := types.EmptyJID
		if chat.Server == types.GroupServer {
			sender = senderJID(client, user)
		}
		if err := client.MarkRead(context.Background(), ids, time.Now(), chat, sender); err != nil {
			return 0, fmt.Errorf("marking read: %v", err)
		}
	}
	setChatRead(store, chatJID, time.Now())
	return len(msgs), nil
}

// ---------- Quoted replies ----------

// buildQuote makes the context that turns a text into a reply to message replyTo of the chat.
func buildQuote(client *whatsmeow.Client, store *MessageStore, chat types.JID, replyTo string) (*waProto.ContextInfo, error) {
	var sender, content string
	var isFromMe bool
	err := store.db.QueryRow("SELECT sender, COALESCE(content, ''), is_from_me FROM messages WHERE id = ? AND chat_jid = ?",
		replyTo, chat.String()).Scan(&sender, &content, &isFromMe)
	if err != nil {
		return nil, fmt.Errorf("message %s not found in this chat", replyTo)
	}
	var participant types.JID
	switch {
	case isFromMe && client.Store.ID != nil:
		participant = client.Store.ID.ToNonAD()
	case chat.Server != types.GroupServer:
		participant = chat
	default:
		participant = senderJID(client, sender)
	}
	return &waProto.ContextInfo{
		StanzaID:      proto.String(replyTo),
		Participant:   proto.String(participant.String()),
		QuotedMessage: &waProto.Message{Conversation: proto.String(content)},
	}, nil
}

// ---------- REST routes ----------

func registerExtraRoutes(client *whatsmeow.Client, store *MessageStore) {
	// Chats with unread messages, most recent first
	http.HandleFunc("/api/unread", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		days := 7
		fmt.Sscan(r.URL.Query().Get("days"), &days)
		cutoff := time.Now().AddDate(0, 0, -days)
		rows, err := store.db.Query("SELECT jid, COALESCE(name, '') FROM chats WHERE last_message_time >= ?", cutoff)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}
		type chatUnread struct {
			JID      string    `json:"chat_jid"`
			Name     string    `json:"name"`
			Unread   int       `json:"unread"`
			LastTime time.Time `json:"last_message_time"`
		}
		var jids, names []string
		for rows.Next() {
			var j, n string
			if rows.Scan(&j, &n) == nil {
				jids, names = append(jids, j), append(names, n)
			}
		}
		rows.Close()
		out := []chatUnread{}
		for i, j := range jids {
			msgs, err := unreadMessages(store, j)
			if err != nil || len(msgs) == 0 || msgs[0].at.Before(cutoff) {
				continue
			}
			out = append(out, chatUnread{JID: j, Name: names[i], Unread: len(msgs), LastTime: msgs[0].at})
		}
		sort.Slice(out, func(a, b int) bool { return out[a].LastTime.After(out[b].LastTime) })
		json.NewEncoder(w).Encode(map[string]any{"success": true, "chats": out})
	})

	// Mark a chat as read (sends read receipts)
	http.HandleFunc("/api/mark_read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID string `json:"chat_jid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChatJID == "" {
			http.Error(w, "chat_jid is required", http.StatusBadRequest)
			return
		}
		n, err := markChatRead(client, store, req.ChatJID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "message": fmt.Sprintf("Marked %d messages as read", n)})
	})
}
