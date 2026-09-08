# دليل GitHub من الصفر — للعمل من Google Colab

هذا الدليل مكتوب لشخص ما سبق استخدم Git ولا GitHub. اتبعيه بالترتيب، وبتخلصين خلال ٢٠ دقيقة تقريبًا.

> المتطلب في ملف التقييم واضح: المشروع **لازم** يكون مرفوع على GitHub، وموثّق، وبتاريخ commits تدريجي —
> مو رفعة وحدة كبيرة في آخر يوم. هذا الدليل يغطي هذا كله.

---

## الخطوة ١ — تجهيز الحساب

1. افتحي <https://github.com> واعملي حساب (أو سجّلي دخول لحسابك الموجود).
2. فعّلي البريد الإلكتروني من رسالة التفعيل، وإلا بعض الأشياء ما راح تشتغل.
3. أضيفي صورة واسم كامل في `Settings → Public profile`. المدرّب راح يفتح حسابك، فخليه يبان مرتب.

## الخطوة ٢ — إنشاء المستودع (Repository)

1. من الزاوية اليمين فوق: `+` ← `New repository`.
2. عبّي الحقول كذا:

| الحقل | القيمة |
|---|---|
| Repository name | `shopsense-data-platform` |
| Description | `Real-time Kafka → Delta Lakehouse → RAG pipeline. SDAIA Academy capstone.` |
| Public / Private | **Public** (لازم المدرّب يقدر يفتحه) |
| Add a README file | ✅ علّمي عليه |
| Add .gitignore | اتركيه `None` — عندنا ملف جاهز أفضل |

3. اضغطي `Create repository`.
4. انسخي رابط المستودع، بيكون شكله كذا:
   `https://github.com/<اسم-حسابك>/shopsense-data-platform`

## الخطوة ٣ — إنشاء مفتاح الدخول (Personal Access Token)

GitHub ما يقبل كلمة المرور من الأدوات الخارجية. لازم **توكن**. هذا يعوّض عنها.

1. روحي: <https://github.com/settings/tokens> ← `Fine-grained tokens` ← `Generate new token`.
2. عبّي:
   - **Token name:** `colab-capstone`
   - **Expiration:** ٣٠ يوم يكفي
   - **Repository access:** `Only select repositories` ← اختاري `shopsense-data-platform`
   - **Permissions → Repository permissions → Contents:** غيّريها إلى **Read and write**
3. `Generate token`، وبعدها **انسخي التوكن فورًا**. ما راح يظهر لك مرة ثانية.

> ⚠️ التوكن مثل كلمة المرور. لا تكتبينه داخل أي خلية في النوتبوك، ولا ترفعينه على GitHub.
> إذا انكشف بالغلط: ارجعي لنفس الصفحة واضغطي `Revoke` وسوّي واحد جديد.

## الخطوة ٤ — حفظ التوكن داخل Colab بشكل آمن

Colab فيه مكان مخصص للأسرار، ما يُحفظ داخل ملف النوتبوك.

1. في Colab، من الشريط الجانبي على اليسار اضغطي أيقونة **المفتاح 🔑** (`Secrets`).
2. `Add new secret`:
   - **Name:** `GITHUB_TOKEN`
   - **Value:** التوكن اللي نسختيه
   - فعّلي `Notebook access` للنوتبوك اللي تشتغلين عليه
3. أضيفي سر ثاني بنفس الطريقة:
   - **Name:** `GITHUB_USERNAME`
   - **Value:** اسم حسابك في GitHub

بعد كذا افتحي `notebooks/00_push_to_github.ipynb` وشغّليه. هو يقرأ السرّين ويسوي كل شي.

---

## الخطوة ٥ — خطة الـ commits التدريجية

هذي أهم نقطة يخسر فيها الناس درجات: يرفعون كل شي دفعة وحدة، فيصير commit واحد وتاريخ فاضي.
الحل بسيط: كل ما تخلصين جزء، ارفعيه بوحده برسالة واضحة.

استخدمي هذي الخطة كما هي — نوتبوك `00_push_to_github.ipynb` فيه دالة `commit_and_push()` تسهّلها:

| # | الملفات | رسالة الـ commit |
|---|---|---|
| 1 | `.gitignore` | `chore: add .gitignore for data artefacts and secrets` |
| 2 | `README.md` | `docs: add project description, setup and run instructions` |
| 3 | `docs/architecture.md` | `docs: document pipeline architecture and components` |
| 4 | `notebooks/01_ingestion_kafka.ipynb` (قبل التشغيل) | `feat(ingestion): add Kafka producer/consumer with Pydantic data contract` |
| 5 | `data/contracts/order_event.schema.json` | `feat(ingestion): export the OrderEvent JSON schema` |
| 6 | `notebooks/01_...` (بعد التشغيل) + `reports/ingestion_report_*.json` | `test(ingestion): executed run — 22% of records routed to the dead-letter topic` |
| 7 | `notebooks/02_delta_lakehouse.ipynb` (قبل التشغيل) | `feat(lakehouse): add Bronze/Silver/Gold layers with Delta MERGE on order_id` |
| 8 | `notebooks/02_...` (بعد التشغيل) + `reports/lakehouse_report_*.json` | `test(lakehouse): executed run — upsert metrics and refused bad writes captured` |
| 9 | `data/knowledge_base/` | `feat(rag): add the support knowledge base corpus` |
| 10 | `notebooks/03_rag_pipeline.ipynb` (قبل التشغيل) | `feat(rag): hybrid retrieval with RRF and cross-encoder reranking` |
| 11 | `notebooks/03_...` (بعد التشغيل) + `reports/rag_report_*.json` | `test(rag): executed run — Hit@3 and MRR across four retrieval strategies` |
| 12 | `README.md` | `docs: add expected output and rubric coverage table` |

**قواعد سريعة لرسائل الـ commit:**
- بالإنجليزي، فعل في المضارع: `add`, `fix`, `document` — مو `added` ولا `I added`.
- بادئة تقول نوع التغيير: `feat:` ميزة جديدة، `fix:` إصلاح، `docs:` توثيق، `test:` تشغيل/اختبار، `chore:` أعمال تنظيمية.
- تشرح **ليش**، مو بس **إيش**. `update file` رسالة ما تفيد أحد.

---

## طريقة ثانية (أسهل) لرفع النوتبوك نفسه

Colab يقدر يرفع النوتبوك مباشرة بدون أوامر Git:

1. `File` ← `Save a copy in GitHub`
2. أول مرة راح يطلب تصريح لحساب GitHub — وافقي.
3. اختاري المستودع `shopsense-data-platform`، وحطي المسار: `notebooks/01_ingestion_kafka.ipynb`
4. اكتبي رسالة الـ commit في الخانة، واضغطي OK.

هذي ممتازة للنوتبوكات لأنها ترفعها **بمخرجاتها**، وكل حفظة = commit مستقل، فتبني لك تاريخ تدريجي تلقائيًا.
بس للملفات الثانية (README، docs، reports) استخدمي `00_push_to_github.ipynb`.

---

## مشاكل شائعة وحلولها

| المشكلة | السبب | الحل |
|---|---|---|
| `remote: Invalid username or password` | استخدمتي كلمة المرور بدل التوكن | ارجعي للخطوة ٣ |
| `Permission to ... denied` | صلاحية `Contents` في التوكن على `Read` فقط | عدّليها إلى `Read and write` |
| `error: failed to push some refs` | فيه تعديلات على GitHub مو موجودة عندك | نفّذي `!git pull --rebase` ثم ارفعي مرة ثانية |
| `nothing to commit, working tree clean` | ما تغيّر شي فعليًا | تأكدي إنك نسختي الملفات للمجلد الصح قبل الـ commit |
| الملف كبير ومرفوض | ملفات Delta أو الـ vector store | لا ترفعينها — `.gitignore` يستثنيها أصلًا، وهذا مقصود |
| النوتبوك على GitHub بدون مخرجات | حفظتيه بعد ما مسحتي الـ outputs | **لا تسوين** `Clear all outputs` قبل الرفع، المخرجات هي الدليل على التشغيل |

---

## قبل التسليم — قائمة تحقق

- [ ] المستودع **Public** والمدرّب يقدر يفتحه
- [ ] الـ README يشرح: الفكرة، المتطلبات، خطوات التشغيل، والمخرجات المتوقعة
- [ ] اسم البرنامج التدريبي وتواريخ الدفعة مكتوبة في README (استبدلي `<FILL IN>`)
- [ ] رابط <https://github.com/SDAIAAcademy> موجود في README
- [ ] `docs/architecture.md` موجود
- [ ] `.gitignore` موجود ولا يوجد أي توكن أو مفتاح مرفوع
- [ ] عدد الـ commits **١٠ أو أكثر** برسائل واضحة، وموزّعة على أكثر من يوم إن أمكن
- [ ] النوتبوكات الثلاثة مرفوعة **ومخرجاتها ظاهرة**
- [ ] مجلد `reports/` فيه ملفات JSON من تشغيل حقيقي
- [ ] (اختياري، ومُشجَّع) عملتي Star لمستودعات سعودية مميزة وتابعتي حساب SDAIA Academy
