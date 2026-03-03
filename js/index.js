document.addEventListener('DOMContentLoaded', function () {
    loadChangelog();
    loadAllLeaderboards();
    setupCitationCopy();
});

function setupCitationCopy() {
    const copyBtn = document.getElementById('copy-citation-btn');
    if(copyBtn) {
        copyBtn.addEventListener('click', () => {
            const codeBlock = document.getElementById('citation-text');
            navigator.clipboard.writeText(codeBlock.innerText).then(() => {
                copyBtn.innerText = 'Copied!';
                setTimeout(() => {
                    copyBtn.innerText = 'Copy';
                }, 1500);
            }).catch(err => {
                console.error('Failed to copy text: ', err);
            });
        });
    }
}

async function loadChangelog() {
    /** Changelog items should look like
    {
        "date": "Mar 3, 2026",
        "number": "1",
        "new": [
          "Foobar"
        ],
        "fixes": [
          "Foobar"
        ],
        "notes": [
          "Foobar"
        ]
    }
    **/
    try {
        const response = await fetch('jsons/changelog.json');
        const changelog = await response.json();
        const logBody = document.getElementById('changelog-body');
        if (!logBody) return;

        changelog.forEach(entry => {
            const div = document.createElement('div');
            div.classList.add('box', 'content', 'mt-3', 'p-3');

            const titleContainer = document.createElement('div');
            titleContainer.className = 'changelog-header';

            const h5 = document.createElement('h5');
            h5.textContent = `Update ${entry.number}`;

            const dateBadge = document.createElement('span');
            dateBadge.className = 'badge ms-2';
            dateBadge.textContent = entry.date;

            titleContainer.appendChild(h5);
            titleContainer.appendChild(dateBadge);
            div.appendChild(titleContainer);

            if (entry.new) {
                div.innerHTML += '<p class="mt-2 mb-1"><strong>✨ New:</strong></p>';
                const ul = document.createElement('ul');
                entry.new.forEach(item => {
                    ul.innerHTML += `<li>${item}</li>`;
                });
                div.appendChild(ul);
            }
            if (entry.fixes) {
                div.innerHTML += '<p class="mt-2 mb-1"><strong>🐞 Fixes:</strong></p>';
                const ul = document.createElement('ul');
                entry.fixes.forEach(item => {
                    ul.innerHTML += `<li>${item}</li>`;
                });
                div.appendChild(ul);
            }
             if (entry.notes) {
                div.innerHTML += '<p class="mt-2 mb-1"><strong>📝 Notes:</strong></p>';
                const ul = document.createElement('ul');
                entry.notes.forEach(item => {
                    ul.innerHTML += `<li>${item}</li>`;
                });
                div.appendChild(ul);
            }
            logBody.appendChild(div);
        });
    } catch (error) {
        console.error('Error loading changelog data:', error);
    }
}

async function loadAllLeaderboards() {
    try {
        const [enResponse, nlResponse] = await Promise.all([
            fetch('jsons/leaderboard-en.json'),
            fetch('jsons/leaderboard-nl.json')
        ]);
        const enData = await enResponse.json();
        const nlData = await nlResponse.json();

        buildLeaderboard('en', 'qg', 'Query generation', enData['Query generation'], ['Rank', 'Method', 'EX', 'obsF1']);
        buildLeaderboard('en', 'e2e', 'End-to-end QA', enData['End-to-end QA'], ['Rank', 'Method', 'EX', 'obsF1']);
        buildLeaderboard('en', 'tr', 'Table retrieval', enData['Table Retrieval'], ['Rank', 'Method', 'acc@2', 'acc@5', 'acc@10']);
        
        buildLeaderboard('nl', 'qg', 'Query generation', nlData['Query generation'], ['Rank', 'Method', 'EX', 'obsF1']);
        buildLeaderboard('nl', 'e2e', 'End-to-end QA', nlData['End-to-end QA'], ['Rank', 'Method', 'EX', 'obsF1']);
        buildLeaderboard('nl', 'tr', 'Table retrieval', nlData['Table Retrieval'], ['Rank', 'Method', 'acc@2', 'acc@5', 'acc@10']);

    } catch (error) {
        console.error('Error loading leaderboard data:', error);
    }
}

function buildLeaderboard(lang, type, leaderboardKey, data, headers) {
    const head = document.getElementById(`leaderboard-head-${lang}-${type}`);
    const body = document.getElementById(`leaderboard-body-${lang}-${type}`);
    if (!head || !body) return;

    // Clear previous content
    head.innerHTML = '';
    body.innerHTML = '';
    
    // Create headers
    let headerRow = '<tr>';
    headers.forEach(h => headerRow += `<th>${h}</th>`);
    headerRow += '</tr>';
    head.innerHTML = headerRow;

    // Calculate scores and sort
    const sortedData = data.map(entry => {
        let score = 0;
        if (leaderboardKey === 'Table retrieval') {
            score = (entry['acc@2'] + entry['acc@5'] + entry['acc@10']) / 3;
        } else {
            score = (entry.EX + entry.obsF1) / 2;
        }
        return { ...entry, score };
    }).sort((a, b) => b.score - a.score);

    // Populate table
    sortedData.forEach((entry, index) => {
        const rank = index + 1;
        const row = document.createElement('tr');
        
        let cells = `<td class="rank-cell"><b>${rank}</b><br><span class="badge bg-secondary">${entry.date}</span></td>`;
        
        headers.slice(1).forEach(header => {
             if(header.toLowerCase() === 'method') {
                 cells += `<td>${entry.method}</td>`;
             } else {
                 cells += `<td>${entry[header] !== undefined ? entry[header].toFixed(2) : 'N/A'}</td>`;
             }
        });
        
        row.innerHTML = cells;
        body.appendChild(row);
    });
}
