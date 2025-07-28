# `biofilter.log` - Understanding  Logs

> 📁 Subpage of: Settings & System Behavior
> 
> 
> 🧠 Audience: Users and developers configuring their working environment
> 

---

## 📌 What Is It?

The `biofilter.log` file is automatically created when you interact with Biofilter3R (e.g., running ETL, querying, or managing the database). It is an essential tool for tracking operations, debugging issues, and auditing system behavior.

---

### 🔍 Where is it located?

By default, the log file is stored in the root directory of the active environment — the same place from where you execute commands like:

```bash
biofilter report list
```

You should see a file named:

```
biofilter.log
```

---

### 🛠️ What does it log?

The log includes:

| Level | Description |
| --- | --- |
| INFO | General events, such as database connection, ETL steps, etc. |
| WARNING | Issues that didn’t stop the process but might need attention. |
| ERROR | Problems that prevented the current operation. |
| DEBUG | (If enabled) Extra information for developers and debugging. |

Example log entries:

```
2025-07-25 09:30:12 [INFO] Creating Biofilter database at sqlite:///my.db
2025-07-25 09:30:13 [INFO] ✅ ETL update process finished.
2025-07-25 09:32:01 [WARNING] Entity already exists with different identifiers.
2025-07-25 09:32:02 [ERROR] Database not connected. Use connect_db() first.
```

---

### 📟 When is it useful?

- ✨ **After a failed ETL**: check what went wrong and where.
- ⌚ **To confirm DTP behavior**: see which steps were executed.
- 🧪 **During debugging**: trace fine-grained operations.
- ✍️ **Auditing**: track when updates or schema changes were performed.

---

### 📌 Can I delete it?

Yes. The log file can be safely deleted at any time. A new one will be created the next time Biofilter3R is used.

However, we recommend keeping it for historical and debugging purposes.

---

### 🧪 Tip

If running on a cluster or server, you may want to rotate logs or direct them to a persistent location for long-term tracking.

---

### 🧠 Note

> As Biofilter3R is still under development, the structure and content of the log may change in future versions.
>