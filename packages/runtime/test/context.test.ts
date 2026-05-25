import { describe, expect, it } from 'vitest';
import { composeSystemPrompt, discoverSessionContext } from '../src/context.ts';
import type { SessionEnv, SkillReference } from '../src/types.ts';

function createWorkspaceSkillEnv(skillMarkdown: string): SessionEnv {
	const skillPath = '/workspace/.agents/skills/review/SKILL.md';
	return {
		cwd: '/workspace',
		resolvePath: (path) => path,
		exec: async () => ({ stdout: '', stderr: '', exitCode: 0 }),
		readFile: async (path) => path === skillPath ? skillMarkdown : '',
		readFileBuffer: async () => new Uint8Array(),
		writeFile: async () => {},
		stat: async (path) => ({ isFile: path === skillPath, isDirectory: path.endsWith('/review'), isSymbolicLink: false, size: 0, mtime: new Date(0) }),
		readdir: async (path) => path === '/workspace/.agents/skills' ? ['review'] : [],
		exists: async (path) => ['/workspace/.agents/skills', '/workspace/.agents/skills/review', skillPath].includes(path),
		mkdir: async () => {},
		rm: async () => {},
	};
}

describe('composeSystemPrompt', () => {
	it('places agent instructions before discovered workspace context', () => {
		const prompt = composeSystemPrompt(
			'Workspace guidance.',
			{},
			{ cwd: '/workspace' },
			'Agent instructions.',
		);

		expect(prompt.indexOf('Agent instructions.')).toBeLessThan(prompt.indexOf('Workspace guidance.'));
		expect(prompt).toContain('Working directory: /workspace');
	});
});

describe('workspace skill discovery', () => {
	it('advertises valid skill metadata without loading the instruction body into context', async () => {
		const context = await discoverSessionContext(createWorkspaceSkillEnv('---\nname: review\ndescription: Reviews changes.\n---\nSecret instructions.'));

		expect(context.skills.review).toEqual({ name: 'review', description: 'Reviews changes.' });
		expect(context.systemPrompt).toContain('Reviews changes.');
		expect(context.systemPrompt).not.toContain('Secret instructions.');
	});

	it('rejects workspace skills that violate the Agent Skills metadata contract', async () => {
		await expect(discoverSessionContext(createWorkspaceSkillEnv('---\nname: other\ndescription: Reviews changes.\n---\nReview.'))).rejects.toThrow('requires it to match directory "review"');
	});

	it('rejects a workspace skill that collides with a registered packaged skill name', async () => {
		const registered: SkillReference = { __flueSkillReference: true, id: 'skill:review:fixture', name: 'review', description: 'Packaged review.' };

		await expect(discoverSessionContext(createWorkspaceSkillEnv('---\nname: review\ndescription: Workspace review.\n---\nReview.'), undefined, [registered])).rejects.toThrow('appears in both agent definition and workspace discovery');
	});
});
